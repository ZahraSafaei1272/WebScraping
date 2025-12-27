import time
from selenium import webdriver
import chromedriver_autoinstaller
from selenium.webdriver.common.by import By
import pandas as pd
import os

# Automatically install the correct version of ChromeDriver
chromedriver_autoinstaller.install()

# IMDb URLs are split into 3 date ranges to avoid memory overload ("Aw, Snap!" errors)
urls = [
    'https://www.imdb.com/search/title/?title_type=feature&release_date=2018-05-01,2018-10-01&count=200',
    'https://www.imdb.com/search/title/?title_type=feature&release_date=2018-10-02,2019-01-01&count=200',
    'https://www.imdb.com/search/title/?title_type=feature&release_date=2019-01-02,2019-05-01&count=200'
]

# File where all results will be appended
output_file = 'imdb_titles_links.csv'

def imdb_driver_setup(url):
    """
    Launch a Chrome browser using Selenium and open the given IMDb URL.
    The function returns the active driver object.
    """
    driver = webdriver.Chrome()
    driver.get(url)
    driver.implicitly_wait(10)  # Wait up to 10 seconds for page elements to appear
    return driver


def imdb_page_loader(driver, max_pages=500):
    """
    Scrolls through the IMDb page and clicks 'Load More' repeatedly
    until all results are displayed or the page reaches max_pages.
    This prevents premature stopping due to lazy loading.
    """
    page = 1
    last_height = 0  # Used to track the page height to detect new content loading

    while page <= max_pages:
        try:
            # Locate the "load more" button
            load_more = driver.find_element(By.XPATH, '//span[@class="ipc-see-more__text"]')

            # Scroll to the button before clicking
            driver.execute_script("arguments[0].scrollIntoView();", load_more)
            time.sleep(2)  # Wait for smooth scrolling and button visibility

            # Click the "load more" button using JavaScript
            driver.execute_script("arguments[0].click();", load_more)
            print(f"Page {page} loaded")
            page += 1

            # Wait for new movie data to fully load before continuing
            time.sleep(5)

            # Check if the page height changed (new content added)
            new_height = driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height:
                print("Reached the end of the list — no more movies to load.")
                break
            last_height = new_height
        except:
            # If the button is missing, perform full scroll down to ensure all movies are visible
            print("No 'load more' button detected. Performing full scroll to bottom...")
            while True:
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(2)
                new_height = driver.execute_script("return document.body.scrollHeight")
                if new_height == last_height:
                    print("End of page reached.")
                    break
                last_height = new_height
            break


def extract_titles(driver):
    """
    Extracts all movie titles and their corresponding IMDb links
    from the fully loaded search results page.
    """
    titles, links = [], []
    blocks = driver.find_elements(By.XPATH, '//li[@class="ipc-metadata-list-summary-item"]')
    print(f"Found {len(blocks)} movies on this segment.")

    for block in blocks:
        try:
            # Extract movie title text
            title_number = block.find_element(By.XPATH, './/h3[@class = "ipc-title__text ipc-title__text--reduced"]')
            title = title_number.text.split('. ', 1)[1]

            # Extract IMDb link for the movie
            link = block.find_element(By.CSS_SELECTOR, 'a').get_attribute('href')

            # Append to the global lists
            titles.append(title)
            links.append(link)
        except Exception:
            # Skip any malformed entries without stopping the script
            continue

    print(f"Extracted {len(blocks)} items from current page.\n")
    return titles, links

def save_to_csv(titles, links, filename):
    """Append titles and links to the CSV file."""
    data = pd.DataFrame({
        "movie_name": titles,
        "link": links
    })
    # Append data without overwriting previous entries
    data.to_csv(filename, mode='a', index=False, header=not os.path.exists(filename))
    print(f"Saved {len(data)} records to {filename}")

# ---- Main Execution ----
for idx, url in enumerate(urls, 1):
    print(f"\n=== Processing URL segment {idx} ===")
    driver = imdb_driver_setup(url)
    imdb_page_loader(driver)
    titles, links = extract_titles(driver)
    driver.quit()
    save_to_csv(titles, links, output_file)
    print(f"Segment {idx} complete.\n")
    time.sleep(3)

print(f"All data successfully saved to {output_file}")