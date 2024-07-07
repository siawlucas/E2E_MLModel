import pandas as pd
from sqlalchemy import create_engine, Table, MetaData, Column, Float, String, Date
from sqlalchemy.exc import SQLAlchemyError
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error
from datetime import datetime

# Configuration
DB_CONFIG = {
    'user': 'admin',
    'password': 'admin',
    'host': 'localhost',
    'port': '5400',
    'database': 'bfi_test'
}

CLEANED_TABLE_NAME = 'product'
RECOMMENDATION_TABLE_NAME = 'pricerecommendation'

def get_db_connection(config):
    """Create and return a database connection."""
    db_connection_str = f"postgresql+psycopg2://{config['user']}:{config['password']}@{config['host']}:{config['port']}/{config['database']}"
    return create_engine(db_connection_str)

def load_cleaned_data(engine, table_name):
    """Load cleaned data from the specified table in the database."""
    query = f"SELECT * FROM {table_name}"
    return pd.read_sql(query, engine)

def prepare_data_by_category(data):
    """Prepare data for regression model by category."""
    categories = data['category'].unique()
    data_by_category = {category: data[data['category'] == category] for category in categories}
    return data_by_category

def train_model(X_train, y_train):
    """Train a linear regression model."""
    model = LinearRegression()
    model.fit(X_train, y_train)
    return model

def evaluate_model(model, X_test, y_test):
    """Evaluate the model and return the mean squared error."""
    y_pred = model.predict(X_test)
    return mean_squared_error(y_test, y_pred)

def recommend_price(originalprice, model):
    """Recommend a price based on the original price using the trained model."""
    return model.predict(pd.DataFrame([[originalprice]], columns=['originalprice']))[0]

def save_recommendations_to_db(engine, recommendations, table_name):
    """Save recommendations to the specified table in the database."""
    metadata = MetaData()
    recommendations_table = Table(table_name, metadata,
                                  Column('productmasterid', String, primary_key=True),
                                  Column('category', String),
                                  Column('price', Float),
                                  Column('date', Date))
    metadata.create_all(engine)  # Create table if it doesn't exist

    try:
        with engine.begin() as connection:  # Using `begin` ensures a commit at the end of the block
            for recommendation in recommendations:
                ins = recommendations_table.insert().values(
                    productmasterid=recommendation['productmasterid'], 
                    category=recommendation['category'],
                    price=float(recommendation['price']),
                    date=recommendation['date']
                )
                print(f"Inserting: {recommendation}")  # Debugging line
                connection.execute(ins)
        print(f"Recommendations saved to table '{table_name}'.")
    except SQLAlchemyError as e:
        print(f"Error saving recommendations to the database: {e}")

def main():
    try:
        engine = get_db_connection(DB_CONFIG)
        cleaned_data = load_cleaned_data(engine, CLEANED_TABLE_NAME)
        print("Cleaned data loaded successfully.")
        print(cleaned_data.head())
        
        data_by_category = prepare_data_by_category(cleaned_data)
        
        models = {}
        recommendations = []
        for category, data in data_by_category.items():
            if len(data) > 1:
                X = data[['originalprice']]
                y = data['price']
                X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
                
                model = train_model(X_train, y_train)
                mse = evaluate_model(model, X_test, y_test)
                print(f"Category: {category}, Mean Squared Error: {mse}")
                
                models[category] = model
                
                # Recommend price for each product in the category
                for index, row in data.iterrows():
                    recommended_price = recommend_price(row['originalprice'], model)
                    recommendations.append({
                        'productmasterid': row['productmasterid'],
                        'category': category,
                        'price': recommended_price,
                        'date': datetime.now().date()
                    })
                    print(f"ProductMasterID: {row['productmasterid']}, Category: {category}, Recommended Price: {recommended_price}, Date: {datetime.now().date()}")
            else:
                print(f"Category: {category} does not have enough data for train-test split.")
        
        # Check if recommendations list is populated
        if not recommendations:
            print("No recommendations to save.")
        else:
            print(f"Total recommendations: {len(recommendations)}")
            # Save recommendations to PostgreSQL
            save_recommendations_to_db(engine, recommendations, RECOMMENDATION_TABLE_NAME)
    except SQLAlchemyError as e:
        print(f"Error in database operations: {e}")

if __name__ == "__main__":
    main()
