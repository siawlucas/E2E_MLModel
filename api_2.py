from fastapi import FastAPI, HTTPException, Depends
from sqlalchemy import create_engine, MetaData, Table, select
from sqlalchemy.orm import sessionmaker
from pydantic import BaseModel
from typing import List

app = FastAPI()

# Database configuration
DB_CONFIG = {
    'user': 'admin',
    'password': 'admin',
    'host': 'localhost',
    'port': '5400',
    'database': 'bfi_test',
    'recommendation_table': 'pricerecommendation'
}

DATABASE_URL = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"

# SQLAlchemy setup
engine = create_engine(DATABASE_URL)
metadata = MetaData()
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Example table model
class PriceRecommendation(BaseModel):
    id: int
    product_name: str
    recommended_price: float
    # Add other fields as necessary

# Dependency to get DB session
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@app.get("/recommendations/", response_model=List[PriceRecommendation])
async def read_recommendations(skip: int = 0, limit: int = 10, db: SessionLocal = Depends(get_db)):
    recommendation_table = Table(DB_CONFIG['recommendation_table'], metadata, autoload_with=engine)
    query = select([recommendation_table]).offset(skip).limit(limit)
    result = db.execute(query).fetchall()
    if not result:
        raise HTTPException(status_code=404, detail="Recommendations not found")
    recommendations = [dict(row) for row in result]
    return recommendations

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
