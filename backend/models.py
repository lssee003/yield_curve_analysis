from sqlalchemy import Column, Integer, Float, Date
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class YieldCurve(Base):
    __tablename__ = "bond_yields"

    id = Column(Integer, primary_key=True, index=True)
    date = Column(Date, unique=True, nullable=False)
    yield_3m = Column(Float)
    yield_6m = Column(Float)
    yield_1y = Column(Float)
    yield_2y = Column(Float)
    yield_3y = Column(Float)
    yield_5y = Column(Float)
    yield_7y = Column(Float)
    yield_10y = Column(Float)
    yield_30y = Column(Float)
