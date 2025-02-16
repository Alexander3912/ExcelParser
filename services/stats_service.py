import statistics
from sqlalchemy.orm import Session
from sqlalchemy import func
from models import ProcessedFile, Check, Product
from logging_config import setup_logger

logger = setup_logger(__name__)

class StatsService:
    def __init__(self, db: Session):
        self.db = db

    def get_total_files(self) -> int:
        try:
            total = self.db.query(func.count(ProcessedFile.id)).scalar()
            logger.debug("Total number of files: %s", total)
            return total
        except Exception as e:
            logger.exception("Error getting files count: %s", e)
            raise

    def get_total_checks(self) -> int:
        try:
            total = self.db.query(func.count(Check.id)).scalar()
            logger.debug("Total number of checks: %s", total)
            return total
        except Exception as e:
            logger.exception("Error while getting the number of checks: %s", e)
            raise

    def get_avg_check_sum(self) -> float:
        try:
            total_sum = self.db.query(func.sum(Product.price)).scalar() or 0
            total_checks = self.get_total_checks()
            avg = total_sum / total_checks if total_checks else 0
            logger.debug("Average checks amount: %s (total amount: %s, checks: %s)", avg, total_sum, total_checks)
            return avg
        except Exception as e:
            logger.exception("Error calculating average checks amount: %s", e)
            raise

    def get_median_product_price(self) -> float:
        try:
            product_prices = [p.price for p in self.db.query(Product).all() if p.price is not None]
            median_price = statistics.median(product_prices) if product_prices else 0
            logger.debug("Median price of the products: %s", median_price)
            return median_price
        except Exception as e:
            logger.exception("Error in calculating the median price of the products: %s", e)
            raise

    def get_total_unique_products(self) -> int:
        try:
            total = self.db.query(Product.name).distinct().count()
            logger.debug("Number of unique products: %s", total)
            return total
        except Exception as e:
            logger.exception("Error getting number of unique products: %s", e)
            raise

    def get_total_sold_quantity(self) -> int:
        try:
            total = self.db.query(func.sum(Product.quantity)).scalar() or 0
            logger.debug("Total number of products sold: %s", total)
            return total
        except Exception as e:
            logger.exception("Error getting total quantity of products sold: %s", e)
            raise

    def get_top_5_products(self) -> list:
        try:
            top_products_query = self.db.query(
                Product.name,
                func.sum(Product.quantity).label("total_quantity")
            ).group_by(Product.name).order_by(func.sum(Product.quantity).desc()).limit(5).all()
            top_products = [{"name": prod.name, "total_quantity": prod.total_quantity} for prod in top_products_query]
            logger.debug("Top 5 products: %s", top_products)
            return top_products
        except Exception as e:
            logger.exception("Error while getting top 5 products: %s", e)
            raise

    def get_stats(self) -> dict:
        try:
            stats = {
                "total_files": self.get_total_files(),
                "total_checks": self.get_total_checks(),
                "avg_check_sum": self.get_avg_check_sum(),
                "median_product_price": self.get_median_product_price(),
                "total_unique_products": self.get_total_unique_products(),
                "total_sold_quantity": self.get_total_sold_quantity(),
                "top_5_products": self.get_top_5_products(),
            }
            logger.info("Statistics collected: %s", stats)
            return stats
        except Exception as e:
            logger.exception("Error collecting statistics: %s", e)
            raise