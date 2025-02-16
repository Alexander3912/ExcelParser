from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from datetime import datetime, timezone

Base = declarative_base()

class ProcessedFile(Base):
    __tablename__ = "processed_files"
    id = Column(Integer, primary_key=True, index=True)
    filename = Column(String, unique=True, index=True)
    file_hash = Column(String, unique=True, index=True)
    processed_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    checks = relationship("Check", back_populates="processed_file")

class Check(Base):
    __tablename__ = "checks"
    id = Column(Integer, primary_key=True, index=True)
    check_identifier = Column(String, index=True)
    date = Column(DateTime(timezone=True))
    operation_type = Column(String)
    processed_file_id = Column(Integer, ForeignKey("processed_files.id"))
    processed_file = relationship("ProcessedFile", back_populates="checks")
    products = relationship("Product", back_populates="check")

class Product(Base):
    __tablename__ = "products"
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, index=True)
    quantity = Column(Integer)
    price = Column(Float)
    check_id = Column(Integer, ForeignKey("checks.id"))
    check = relationship("Check", back_populates="products")