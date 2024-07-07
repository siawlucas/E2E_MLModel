from fastapi import FastAPI, HTTPException
from sqlalchemy import create_engine, Table, MetaData, select
from sqlalchemy.orm import sessionmaker

# Configuration
DB_CONFIG = {
    'user': 'admin',
    'password': 'admin',
    'host': 'localhost',
    'port': '5400',
    'database': 'bfi_test',
    'recommendation_table': 'pricerecommendation'
}

# Initialize FastAPI app
app = FastAPI()

# Database connection
def get_db_connection():
    """Create and return a database connection."""
    db_connection_str = f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    engine = create_engine(db_connection_str)
    return engine

# Database engine and session
engine = get_db_connection()
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Metadata object
metadata = MetaData()

# Define the price recommendations table
price_recommendations = Table(
    DB_CONFIG['recommendation_table'], metadata, autoload_with=engine
)

@app.get("/recommendations/{category}")
async def get_recommendation(category: str):
    """Get the price recommendation for a given category."""
    session = SessionLocal()
    stmt = select(price_recommendations).where(price_recommendations.c.category == category)
    result = session.execute(stmt).fetchone()
    session.close()
    
    if result:
        recommendation = {
            "category": result.category,
            "recommended_price": result.recommended_price
        }
        return recommendation
    else:
        raise HTTPException(status_code=404, detail="Category not found")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000, reload=True)
