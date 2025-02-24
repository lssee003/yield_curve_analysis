from datetime import date
from typing import List

from fastapi import FastAPI, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select

from backend.database import get_db
from backend.models import YieldCurve

app = FastAPI()


@app.get("/")
def home():
    return {"message": "Yield Curve API is running!"}


# Fetch all yield curve data
@app.get("/yields", response_model=List[dict])
async def get_yield_curves(db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(YieldCurve))
    data = result.scalars().all()
    return [{"date": y.date, "3m": y.yield_3m, "6m": y.yield_6m, "1y": y.yield_1y, "2y": y.yield_2y, "3y": y.yield_3y,
             "5y": y.yield_5y, "7y": y.yield_7y, "10y": y.yield_10y, "30y": y.yield_30y} for y in data]


# Fetch yield curve by date
@app.get("/yields/{date}")
async def get_yield_by_date(date: date, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(YieldCurve).where(YieldCurve.date == date))
    data = result.scalars().first()
    if data:
        return {"date": data.date, "3m": data.yield_3m, "6m": data.yield_6m, "1y": data.yield_1y, "2y": data.yield_2y,
                "3y": data.yield_3y, "5y": data.yield_5y, "7y": data.yield_7y, "10y": data.yield_10y,
                "30y": data.yield_30y}
    return {"error": "Data not found"}
