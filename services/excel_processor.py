import io
import hashlib
import pandas as pd
from datetime import datetime, timezone
from fastapi import HTTPException
from models import ProcessedFile, Check, Product
from logging_config import setup_logger

logger = setup_logger(__name__)

class ExcelProcessor:
    async def process_upload(self, file, db):
        try:
            logger.debug("Start processing file: %s", file.filename)
            
            supported_extensions = ('.xls')
            if not file.filename.lower().endswith(supported_extensions):
                logger.error("Unsupported file format: %s", file.filename)
                raise HTTPException(status_code=400, detail="Unsupported file format. Please upload an Excel file (.xls)")
            
            contents = await file.read()
            logger.debug("File read successfully, size: %d byte", len(contents))
            
            file_hash = hashlib.sha256(contents).hexdigest()
            logger.debug("File hash calculated: %s", file_hash)
            
            df_full = self._read_excel(contents)
            logger.debug("Excel file read, DataFrame size: %d rows", len(df_full))
            if len(df_full) < 3:
                logger.error("The Excel file does not contain enough rows to process")
                raise HTTPException(status_code=400, detail="The file has insufficient structure")
            
            header_index = self._find_header_index(df_full)
            if header_index == -1:
                logger.error("Unable to determine file title.")
                raise HTTPException(status_code=400, detail="Unable to determine file title")
            logger.debug("The title was found on the line with index: %d", header_index)
            
            if len(df_full) < header_index + 3:
                logger.error("There are not enough rows for data after the header")
                raise HTTPException(status_code=400, detail="The file has an incorrect data structure")
            
            df_data = df_full.iloc[header_index + 2:].reset_index(drop=True)
            logger.debug("Data start defined, number of data rows: %d", len(df_data))
            
            processed_file = self._get_or_update_processed_file(file.filename, file_hash, db)
            logger.debug("File record received or updated, ID: %s", processed_file.id)
            
            self._process_data_rows(df_data, processed_file, db)
            logger.info("File %s processed successfully", file.filename)
            return {"detail": "The file was processed successfully"}
        except Exception as e:
            if isinstance(e, HTTPException):
                logger.error("HTTPException: %s", e.detail)
                raise e
            logger.exception("Error processing file: %s", e)
            raise HTTPException(status_code=500, detail=str(e))

    def _read_excel(self, contents):
        excel_bytes = io.BytesIO(contents)
        return pd.read_excel(excel_bytes, header=None, engine='xlrd')

    def _find_header_index(self, df, keywords=None, threshold=3):
        if keywords is None:
            keywords = ["Номер чека", "Чек", "Операція"]
        for idx, row in df.iterrows():
            row_values = row.astype(str).tolist()
            match_count = sum(1 for keyword in keywords if any(keyword.lower() in cell.lower() for cell in row_values))
            if match_count >= threshold:
                return idx
        return -1

    def _get_or_update_processed_file(self, filename, file_hash, db):
        processed_file = db.query(ProcessedFile).filter(ProcessedFile.filename == filename).first()
        if processed_file:
            processed_file.file_hash = file_hash
            processed_file.processed_at = datetime.now(timezone.utc)
            db.commit()
            db.refresh(processed_file)
            logger.debug("Updated entry for file named: %s", filename)
        else:
            processed_file = ProcessedFile(filename=filename, file_hash=file_hash)
            db.add(processed_file)
            db.commit()
            db.refresh(processed_file)
            logger.debug("The record was created for a file named: %s", filename)
        return processed_file

    def _process_data_rows(self, df_data, processed_file, db):
        current_check = None
        for index, row in df_data.iterrows():
            first_cell = str(row[0])
            if first_cell.startswith("Чек"):
                current_check = self._create_check_from_row(first_cell, row, processed_file, db)
                logger.debug("Check processed: %s (ID: %s)", first_cell, current_check.id)
            else:
                if current_check is not None:
                    self._create_product_from_row(first_cell, row, current_check, db)
                    logger.debug("Product processed for check ID %s: %s", current_check.id, first_cell)

    def _create_check_from_row(self, first_cell, row, processed_file, db):
        existing_check = db.query(Check).filter(Check.check_identifier == first_cell).first()
        if existing_check:
            logger.debug("Check %s already exists, returning it", first_cell)
            return existing_check
        check_date = self._parse_check_date(first_cell)
        new_check = Check(
            check_identifier=first_cell,
            date=check_date,
            operation_type=str(row[5]) if len(row) > 5 else None,
            processed_file_id=processed_file.id
        )
        db.add(new_check)
        db.commit()
        db.refresh(new_check)
        return new_check

    def _create_product_from_row(self, first_cell, row, current_check, db):
        if first_cell.strip().lower() == "разом":
            logger.debug("Skip the line with the final value Разом")
            return
        existing_product = db.query(Product).filter(
            Product.check_id == current_check.id,
            Product.name == first_cell
        ).first()
        if existing_product:
            logger.debug("Product %s for check ID %s already exists, skipping", first_cell, current_check.id)
            return
        try:
            product_quantity = int(row[6]) if len(row) > 6 and pd.notna(row[6]) else 0
        except Exception:
            product_quantity = 0
        try:
            product_price = float(row[7]) if len(row) > 7 and pd.notna(row[7]) else 0.0
        except Exception:
            product_price = 0.0
        product = Product(
            name=first_cell,
            quantity=product_quantity,
            price=product_price,
            check_id=current_check.id
        )
        db.add(product)
        db.commit()

    def _parse_check_date(self, check_str):
        if "від" in check_str:
            try:
                date_str = check_str.split("від")[1].strip()
                return datetime.strptime(date_str, "%d.%m.%Y %H:%M:%S")
            except Exception:
                return None
        return None