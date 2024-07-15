import os
import logging
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
import time
import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, Column, String, Integer, DateTime, Float
from webdriver_manager.chrome import ChromeDriverManager
import psycopg2

# Configuration variables
BASE_URL = "https://www.klikindomaret.com/page/unilever-officialstore?categoryID=&productbrandid=&sortcol=&pagesize=50&startprice=&endprice=&attributes=&ShowItem="
MAX_PAGES = 2  # Set the number of pages to scrape
WAIT_TIME = 5  # Time to wait for pages to load
DATA_DIR = 'data'
TIMESTAMP = datetime.now().strftime("%Y%m%d%H%M%S")
BATCH_SIZE = 5  # Number of products to process before uploading

# PostgreSQL configuration
POSTGRES_HOST = 'localhost'
POSTGRES_PORT = '5400'
POSTGRES_DB = 'e2e_ml'
POSTGRES_USER = 'admin'
POSTGRES_PASSWORD = 'admin'
TABLE_NAME = 'klikindomaret_stg'

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

def setup_driver():
    chrome_options = Options()
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    service = ChromeService(executable_path=ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=chrome_options)

def get_page_source(url, driver, wait_time):
    try:
        driver.get(url)
        time.sleep(wait_time)
        return driver.page_source
    except Exception as e:
        logger.error(f"Error getting page source for URL {url}: {e}")
        return ""

def parse_product_details(soup):
    products = soup.find_all('div', class_='each-item')
    product_details = []

    for product in products:
        try:
            product_name = get_text(product, 'div', 'title')
            product_link = get_link(product, 'a')
            plu = get_attribute(product, 'div', 'wrp-media', 'data-plu')
            
            product_details.append({
                'name': product_name,
                'link': product_link,
                'plu': plu,
                'createdate': datetime.now()
            })
        except Exception as e:
            logger.error(f"Error parsing product details: {e}")
    
    return product_details

def get_text(product, tag, class_name, last_word=False):
    try:
        tag = product.find(tag, class_=class_name)
        if tag:
            text = tag.text.strip()
            return text.split()[-1] if last_word else text
    except Exception as e:
        logger.error(f"Error getting text for tag {tag} with class {class_name}: {e}")
    return ''

def get_price_text(product, tag, class_name):
    try:
        tag = product.find(tag, class_=class_name)
        if tag:
            text = tag.text.strip()
            price = text.split('Rp')[-1].strip()  # Extract the part after 'Rp'
            return f'Rp {price}'
    except Exception as e:
        logger.error(f"Error getting price text for tag {tag} with class {class_name}: {e}")
    return ''

def get_link(product, tag):
    try:
        link_tag = product.find_parent(tag, href=True)
        return f"https://www.klikindomaret.com{link_tag['href']}" if link_tag else ''
    except Exception as e:
        logger.error(f"Error getting link for tag {tag}: {e}")
    return ''

def get_attribute(product, tag, class_name, attribute):
    try:
        attr_tag = product.find(tag, class_=class_name)
        return attr_tag[attribute] if attr_tag and attribute in attr_tag.attrs else ''
    except Exception as e:
        logger.error(f"Error getting attribute {attribute} for tag {tag} with class {class_name}: {e}")
    return ''

def scrape_additional_data(product_link, driver):
    try:
        driver.get(product_link)
        time.sleep(WAIT_TIME)  # Adjust the sleep time as necessary
        page_source = driver.page_source
        soup = BeautifulSoup(page_source, 'html.parser')
        
        # Extracting additional data from the product page
        discount = get_text(soup, 'span', 'discount')
        original_price = get_price_text(soup, 'span', 'strikeout disc-price')
        discounted_price = get_text(soup, 'span', 'normal price-final')
        description = soup.find('span', id='desc-product').text.strip() if soup.find('span', id='desc-product') else ''
        store_info = get_text(soup, 'span', 'typesend-title')
        
        # Extracting the last breadcrumb as category
        breadcrumbs = soup.find('div', class_='breadcrumb')
        category = breadcrumbs.find_all('a')[-1].text.strip() if breadcrumbs else ''
        
        return {
            'discount': discount,
            'original_price': original_price,
            'discounted_price': discounted_price,
            'description': description,
            'store_info': store_info,
            'category': category
        }
    except Exception as e:
        logger.error(f"Error scraping additional data for product link {product_link}: {e}")
        return {}

def create_table(engine):
    metadata = MetaData()
    table = Table(TABLE_NAME, metadata,
                  Column('name', String, nullable=False),
                  Column('link', String, nullable=False),
                  Column('plu', String, nullable=True),
                  Column('discount', String, nullable=True),
                  Column('original_price', String, nullable=True),
                  Column('discounted_price', String, nullable=True),
                  Column('description', String, nullable=True),
                  Column('store_info', String, nullable=True),
                  Column('category', String, nullable=True),
                  Column('createdate', DateTime, nullable=False))
    metadata.create_all(engine)
    logger.info(f"Table {TABLE_NAME} created in PostgreSQL")

def upload_batch_to_postgres(data, engine):
    try:
        df = pd.DataFrame(data)
        df.to_sql(TABLE_NAME, engine, if_exists='append', index=False)
        logger.info(f"Uploaded batch of {len(data)} products to PostgreSQL table {TABLE_NAME}")
    except Exception as e:
        logger.error(f"Error uploading batch to PostgreSQL: {e}")

def scrape_products(base_url, max_pages, driver, wait_time, engine):
    all_product_details = []
    batch = []
    
    for page in range(1, max_pages + 1):
        url = f"{base_url}&page={page}"
        logger.info(f"Scraping page {page}: {url}")
        page_source = get_page_source(url, driver, wait_time)
        if not page_source:
            continue
        soup = BeautifulSoup(page_source, 'html.parser')
        product_details = parse_product_details(soup)
        
        for product in product_details:
            additional_data = scrape_additional_data(product['link'], driver)
            if additional_data:
                product.update(additional_data)
                batch.append(product)
                logger.info(f"Scraped product: {product}")
                if len(batch) >= BATCH_SIZE:
                    upload_batch_to_postgres(batch, engine)
                    batch.clear()
        
        all_product_details.extend(product_details)
    
    # Upload any remaining products in the batch
    if batch:
        upload_batch_to_postgres(batch, engine)
    
    return all_product_details

def main():
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)

    driver = setup_driver()
    engine = create_engine(f'postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}')
    create_table(engine)
    
    try:
        all_product_details = scrape_products(BASE_URL, MAX_PAGES, driver, WAIT_TIME, engine)
    finally:
        driver.quit()

if __name__ == "__main__":
    main()
