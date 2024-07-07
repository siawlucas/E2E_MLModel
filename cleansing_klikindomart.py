import pandas as pd
from sqlalchemy import create_engine, text

# Configuration
DB_CONFIG = {
    'user': 'admin',
    'password': 'admin',
    'host': 'localhost',
    'port': '5400',
    'database': 'bfi_test'
}

TABLE_NAME = 'klikindomaret_ref'
CLEANED_TABLE_NAME = 'product'

def get_db_connection(config):
    """Create and return a database connection."""
    db_connection_str = f"postgresql+psycopg2://{config['user']}:{config['password']}@{config['host']}:{config['port']}/{config['database']}"
    return create_engine(db_connection_str)

def load_data(engine, table_name):
    """Load data from the specified table in the database."""
    query = f"SELECT * FROM {table_name}"
    try:
        return pd.read_sql(query, engine)
    except Exception as e:
        print(f"Error loading data: {e}")
        return None

def read_sql_file(file_path, table_name):
    """Read the SQL query from a file and format it with the table name."""
    try:
        with open(file_path, 'r') as file:
            return file.read().format(table_name=table_name)
    except Exception as e:
        print(f"Error reading SQL file: {e}")
        return None

def clean_data(engine, cleaning_sql):
    """Clean data by removing special characters and ensuring numeric fields are properly formatted."""
    try:
        with engine.connect() as connection:
            print("Executing cleaning SQL query...")
            result = connection.execute(text(cleaning_sql))
            return pd.DataFrame(result.fetchall(), columns=result.keys())
    except Exception as e:
        print(f"An error occurred while cleaning the data: {e}")
        return None

def save_cleaned_data(engine, cleaned_data, cleaned_table_name):
    """Save cleaned data to the specified table in the database."""
    try:
        cleaned_data.to_sql(cleaned_table_name, engine, if_exists='replace', index=False)
        print(f"Cleaned data saved to table '{cleaned_table_name}'.")
    except Exception as e:
        print(f"An error occurred while saving the cleaned data: {e}")

def save_data_to_csv(data, file_name):
    """Save the DataFrame to a CSV file."""
    try:
        data.to_csv(file_name, index=False)
        print(f"Cleaned data saved to '{file_name}'.")
    except Exception as e:
        print(f"An error occurred while saving the data to CSV: {e}")

def main():
    """Main function to load, clean, and save data."""
    engine = get_db_connection(DB_CONFIG)
    
    raw_data = load_data(engine, TABLE_NAME)
    if raw_data is not None:
        print("Data loaded successfully. Sample data:")
        print(raw_data.head())
    else:
        print("Failed to load data.")
        return
    
    cleaning_sql = read_sql_file('cleaning_query.sql', TABLE_NAME)
    if cleaning_sql is None:
        print("Failed to read cleaning SQL query.")
        return
    
    cleaned_data = clean_data(engine, cleaning_sql)
    if cleaned_data is not None:
        save_cleaned_data(engine, cleaned_data, CLEANED_TABLE_NAME)
        save_data_to_csv(cleaned_data, 'cleaned_data.csv')
        print("Data cleaning complete.")
    else:
        print("Data cleaning failed.")

if __name__ == "__main__":
    main()
