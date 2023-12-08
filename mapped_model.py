from sqlalchemy import Column, Integer, Numeric, DateTime, ForeignKey, Text
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class Asset(Base):
    __tablename__ = 'asset'

    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(Text, nullable=False, unique=True)
    min_lot_size = Column(Numeric, nullable=False)
    trading = Column(Integer, nullable=False)
    min_notional = Column(Numeric, nullable=False)

class AssetPrice(Base):
    __tablename__ = 'asset_price'

    asset_id = Column(Integer, ForeignKey('asset.id'), primary_key=True)
    open_time = Column(DateTime, primary_key=True)
    open = Column(Numeric, nullable=False)
    high = Column(Numeric, nullable=False)
    low = Column(Numeric, nullable=False)
    close = Column(Numeric, nullable=False)
    volume = Column(Numeric, nullable=False)
    close_time = Column(DateTime, nullable=False)

    # Define a relationship if needed (assuming 'Asset' is the related table)
    asset = relationship("Asset", backref="asset_prices")  # Adjust as per your schema

