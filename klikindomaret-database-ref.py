import os
import logging
from datetime import datetime
from sqlalchemy import create_engine, MetaData, Table, Column, String, Integer, DateTime, text
import pandas as pd

# PostgreSQL configuration
POSTGRES_HOST = 'localhost'
POSTGRES_PORT = '5400'
POSTGRES_DB = 'e2e_ml'
POSTGRES_USER = 'admin'
POSTGRES_PASSWORD = 'admin'
STG_TABLE_NAME = 'klikindomaret_stg'
REF_TABLE_NAME = 'klikindomaret_ref'

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

def create_ref_table(engine):
    metadata = MetaData()
    table = Table(REF_TABLE_NAME, metadata,
                  Column('id', Integer, primary_key=True),
                  Column('name', String, nullable=False),
                  Column('price', String, nullable=False),
                  Column('originalprice', String, nullable=False),
                  Column('discountpercentage', String, nullable=False),
                  Column('detail', String, nullable=True),
                  Column('platform', String, nullable=False),
                  Column('productmasterid', String, nullable=True),
                  Column('category', String, nullable=True),
                  Column('createdate', DateTime, nullable=False))
    metadata.create_all(engine)
    logger.info(f"Table {REF_TABLE_NAME} created in PostgreSQL")

def run_query_and_store(engine):
    query = f"""
    SELECT id, name, discounted_price as price, 
           CASE WHEN original_price = '' THEN discounted_price ELSE original_price END AS originalprice,
           CASE WHEN discount = '' THEN '0%' ELSE discount END AS discountpercentage,
           description as detail, 'klikindomaret' as platform, plu as productmasterid, category, createdate
    FROM {STG_TABLE_NAME}
    """
    
    try:
        with engine.connect() as connection:
            result = connection.execute(text(query))
            data = result.fetchall()
            df = pd.DataFrame(data, columns=result.keys())
            df.to_sql(REF_TABLE_NAME, engine, if_exists='append', index=False)
            logger.info(f"Data from {STG_TABLE_NAME} successfully stored in {REF_TABLE_NAME}")
    except Exception as e:
        logger.error(f"Error running query and storing data: {e}")

def main():
    engine = create_engine(f'postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}')
    create_ref_table(engine)
    run_query_and_store(engine)

if __name__ == "__main__":
    main()
