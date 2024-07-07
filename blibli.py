import time
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
import pandas as pd
from datetime import datetime
from webdriver_manager.chrome import ChromeDriverManager

# URL provided by the user
url = "https://www.blibli.com/cari/unilever%20indonesia%20official?seller=Official%20Store&category=53400"

# Initialize undetected ChromeDriver with webdriver-manager
options = uc.ChromeOptions()
# options.add_argument('--headless=new')  # Correct way to set headless mode
options.add_argument("--disable-gpu")
options.add_argument("--no-sandbox")
# options.add_argument("--window-size=1920x1080")

# Use webdriver-manager to get the correct ChromeDriver
driver = uc.Chrome(driver_executable_path=ChromeDriverManager().install(), options=options)

# Function to fetch the HTML content of the page
def fetch_html(url):
    driver.get(url)
    time.sleep(10)  # Wait for the page to fully load and bypass any Cloudflare verification
    return driver.page_source

# Function to parse the HTML and extract product details
def parse_html(html):
    driver.get(url)
    time.sleep(10)
    
    products = driver.find_elements(By.CLASS_NAME, 'product__card')
    product_list = []
    
    for product in products:
        try:
            name = product.find_element(By.CLASS_NAME, 'blu-product__name').text.strip()
            price_after = product.find_element(By.CLASS_NAME, 'blu-product__price-after').text.strip()
            price_before = product.find_element(By.CLASS_NAME, 'blu-product__price-before').text.strip() if product.find_elements(By.CLASS_NAME, 'blu-product__price-before') else None
            discount = product.find_element(By.CLASS_NAME, 'blu-product__price-discount').text.strip() if product.find_elements(By.CLASS_NAME, 'blu-product__price-discount') else None
            rating = product.find_element(By.CLASS_NAME, 'blu-product__rating-wrapper').find_element(By.TAG_NAME, 'span').text.strip()
            sold_count = product.find_element(By.CLASS_NAME, 'blu-product__sold').text.strip().replace('Terjual', '').strip()
            store_location = product.find_element(By.CLASS_NAME, 'blu-product__location-text').find_elements(By.TAG_NAME, 'span')[1].text.strip()
            
            product_list.append({
                'Name': name,
                'Price_After': price_after,
                'Price_Before': price_before,
                'Discount': discount,
                'Rating': rating,
                'Sold_Count': sold_count,
                'Store_Location': store_location
            })
        except Exception as e:
            print(f"Error parsing product: {e}")
            continue
    
    return product_list

# Function to save product details to a CSV file
def save_to_csv(product_list):
    df = pd.DataFrame(product_list)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f'blibli_products_{timestamp}.csv'
    df.to_csv(filename, index=False)
    print(f"Data saved to {filename}")

# Main function to execute the script
def main():
    html = fetch_html(url)
    if html:
        product_list = parse_html(html)
        if product_list:
            save_to_csv(product_list)
        else:
            print("No products found")
    else:
        print("Failed to retrieve the webpage")
    driver.quit()

if __name__ == "__main__":
    main()
