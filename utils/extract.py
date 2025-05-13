import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import time
import random
import re

def scrape_page(url, session=None):
    """
    Scrape data from a single page of Fashion Studio website
    
    Args:
        url (str): URL of the page to scrape
        session (requests.Session, optional): Session for making HTTP requests
        
    Returns:
        list: List of dictionaries containing product data
    """
    if session is None:
        session = requests.Session()
    
    try:
        response = session.get(url)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        products_data = []
        
        # Find all product containers
        product_containers = soup.find_all('div', class_='collection-card')
        
        for product in product_containers:
            try:
                # Extract product details
                product_details = product.find('div', class_='product-details')
                if not product_details:
                    continue
                
                # Title
                title_element = product_details.find('h3', class_='product-title')
                title = title_element.text.strip() if title_element else "Unknown Product"
                
                # Price - could be displayed in different ways
                # Check if price is displayed as "Price Unavailable"
                if product_details.find(string="Price Unavailable"):
                    price = "Price Unavailable"
                else:
                    # Look for price with $ symbol
                    price_tags = product_details.find_all(['span', 'p'], string=re.compile(r'\$\d+\.\d+'))
                    if price_tags:
                        price = price_tags[0].text.strip() if price_tags else "Unavailable"
                    else:
                        price = None
                
                # Rating
                rating_text = None
                rating_elements = product_details.find_all('p')
                for element in rating_elements:
                    if 'Rating:' in element.text:
                        rating_text = element.text.strip()
                        break
                
                # Colors
                colors_text = None
                for element in rating_elements:
                    if 'Colors' in element.text and 'Rating' not in element.text:
                        colors_text = element.text.strip()
                        break
                
                # Size
                size_text = None
                for element in rating_elements:
                    if 'Size:' in element.text:
                        size_text = element.text.strip()
                        break
                
                # Gender
                gender_text = None
                for element in rating_elements:
                    if 'Gender:' in element.text:
                        gender_text = element.text.strip()
                        break
                
                # Add timestamp
                timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                
                products_data.append({
                    'Title': title,
                    'Price': price,
                    'Rating': rating_text,
                    'Colors': colors_text,
                    'Size': size_text,
                    'Gender': gender_text,
                    'timestamp': timestamp
                })
                
            except Exception as e:
                print(f"Error extracting product data: {str(e)}")
                continue
        
        return products_data
    
    except requests.exceptions.RequestException as e:
        print(f"Error fetching page {url}: {str(e)}")
        return []

def get_next_page_url(current_url, soup):
    """
    Extract the URL for the next page
    
    Args:
        current_url (str): Current page URL
        soup (BeautifulSoup): Parsed HTML of the current page
        
    Returns:
        str or None: URL for the next page or None if no next page
    """
    try:
        # Find the "Next" link in pagination
        next_link = soup.find('a', class_='page-link', string='Next')
        if next_link and 'href' in next_link.attrs:
            next_href = next_link['href']
            
            # Handle relative URLs
            if next_href.startswith('/'):
                base_url = '/'.join(current_url.split('/')[:3])  # Gets https://domain.com part
                return base_url + next_href
            else:
                return next_href
        return None
    except Exception as e:
        print(f"Error getting next page URL: {str(e)}")
        return None

def scrape_main(base_url="https://fashion-studio.dicoding.dev", max_pages=50):
    """
    Scrape multiple pages from Fashion Studio website
    
    Args:
        base_url (str): Base URL of the website
        max_pages (int): Maximum number of pages to scrape
        
    Returns:
        pandas.DataFrame: DataFrame containing all scraped product data
    """
    all_products = []
    session = requests.Session()
    
    try:
        current_url = base_url
        current_page = 1
        
        while current_page <= max_pages:
            print(f"Scraping page {current_page} of {max_pages}...")
            
            try:
                # Get page content
                response = session.get(current_url)
                response.raise_for_status()
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # Scrape products from current page
                page_products = scrape_page(current_url, session)
                
                if not page_products:
                    print(f"No products found on page {current_page}. Stopping.")
                    break
                    
                all_products.extend(page_products)
                print(f"Products found on page {current_page}: {len(page_products)}")
                
                # Find URL for next page
                next_url = get_next_page_url(current_url, soup)
                
                if not next_url:
                    print("No next page found. Stopping.")
                    break
                    
                current_url = next_url
                current_page += 1
                
                # Random delay to avoid overloading the server
                time.sleep(random.uniform(1, 3))
                
            except requests.exceptions.RequestException as e:
                print(f"Error fetching page {current_page}: {str(e)}")
                break
            except Exception as e:
                print(f"Unexpected error on page {current_page}: {str(e)}")
                break
    
    except Exception as e:
        print(f"Error in main scraping process: {str(e)}")
        return None
    
    # Convert to DataFrame
    df = pd.DataFrame(all_products)
    print(f"Total products scraped: {len(df)}")
    
    return df

if __name__ == "__main__":
    # Test the scraper
    df = scrape_main(max_pages=50)
    if df is not None:
        print(df.head())
        df.to_csv("raw_products.csv", index=False)
    else:
        print("Scraping failed.")