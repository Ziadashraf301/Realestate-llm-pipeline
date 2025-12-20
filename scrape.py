from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import time

def scrape_aqarmap_selenium():
    url = "https://aqarmap.com.eg/ar/for-sale/property-type/alexandria/"

    # --- Chrome options (important for cloud envs) ---
    chrome_options = Options()
    chrome_options.add_argument("--headless=new")   # Headless mode
    chrome_options.add_argument("--no-sandbox")     # Required in Codespaces
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument(
        "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )

    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=chrome_options
    )

    try:
        driver.get(url)

        # Wait until listings load
        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.CLASS_NAME, "listing-card"))
        )

        time.sleep(2)  # allow JS to fully render

        soup = BeautifulSoup(driver.page_source, "html.parser")
        listings = soup.find_all("div", class_="listing-card")

        print(f"Found {len(listings)} listings")

        # Extract property URLs
        property_urls = []
        for card in listings:
            link = card.find("a", href=True)
            if link:
                href = link["href"]
                if href.startswith("/"):
                    href = "https://aqarmap.com.eg" + href
                property_urls.append(href)

        property_urls = list(set(property_urls))
        print("Sample URLs:")
        for url in property_urls[:5]:
            print(url)

    finally:
        driver.quit()

scrape_aqarmap_selenium()
