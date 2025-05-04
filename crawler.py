import requests
from bs4 import BeautifulSoup
import csv
import os
import subprocess
import logging
import time
import random
import re
import hashlib
from urllib.parse import urljoin
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Get settings from environment variables
forum_url = os.getenv("FORUM_URL", "https://pornotorrent.top/forum-1670/")
forum_id = os.getenv("FORUM_ID", "1670")
csv_file = os.getenv("CSV_FILE", f"{forum_id}.csv")
base_url = forum_url.rstrip('/') + '/'
download_base_url = "https://files.cdntraffic.top/PL/torrent/files/"
MAX_RETRIES = 3
RETRY_DELAY = 0.5
COMMIT_INTERVAL = 1000
TIMEOUT = 10
MAX_WORKERS = 5

# Headers for page requests
page_headers = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "Accept-Language": "en,zh-CN;q=0.9,zh;q=0.8",
    "Connection": "keep-alive",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "Sec-Ch-Ua": '"Chromium";v="135", "Not-A.Brand";v="8"',
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": '"macOS"',
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "same-origin",
    "Sec-Fetch-User": "?1",
    "Upgrade-Insecure-Requests": "1",
    "DNT": "1",
    "Referer": "https://pornotorrent.top/"
}

# Headers for torrent file requests
torrent_headers = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "Accept-Language": "en,zh-CN;q=0.9,zh;q=0.8",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "Sec-Ch-Ua": '"Chromium";v="135", "Not-A.Brand";v="8"',
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": '"macOS"',
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Upgrade-Insecure-Requests": "1",
    "DNT": "1",
    "Priority": "u=0, i"
}

# Configure requests session with retries
session = requests.Session()
retries = Retry(total=MAX_RETRIES, backoff_factor=RETRY_DELAY, status_forcelist=[500, 502, 503, 504])
session.mount("https://", HTTPAdapter(max_retries=retries))

def init_csv():
    """Initialize CSV file if it doesn't exist"""
    if not os.path.exists(csv_file):
        with open(csv_file, mode='w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerow(["Page", "Title", "URL", "Publisher", "Link"])
        logging.info(f"Initialized new CSV file: {csv_file}")
        configure_git_lfs()
    else:
        logging.info(f"CSV file '{csv_file}' already exists")

def configure_git_lfs():
    """Configure Git LFS tracking"""
    try:
        subprocess.run(["git", "lfs", "track", csv_file], check=True)
        logging.info(f"Configured Git LFS to track {csv_file}")
    except subprocess.CalledProcessError as e:
        logging.error(f"Error configuring Git LFS: {e.stderr}")
        raise

def git_commit(message):
    """Commit CSV file to Git repository"""
    try:
        subprocess.run(["git", "add", csv_file], check=True)
        result = subprocess.run(["git", "commit", "-m", message], capture_output=True, text=True)
        if result.returncode == 0:
            subprocess.run(["git", "push"], check=True)
            logging.info(f"Git commit successful: {message}")
        else:
            logging.warning(f"No changes to commit: {result.stderr}")
    except subprocess.CalledProcessError as e:
        logging.error(f"Git error: {e.stderr}")

def get_topic_id(url):
    """Extract topic ID from URL"""
    match = re.search(r'/(\d+)-t\.html$', url)
    return match.group(1) if match else None

def get_download_url(topic_id):
    """Construct download URL"""
    return f"{download_base_url}{topic_id}.torrent" if topic_id else ""

def torrent_to_magnet(torrent_url):
    """Convert torrent URL to magnet link"""
    try:
        response = session.get(torrent_url, headers=torrent_headers, timeout=TIMEOUT)
        response.raise_for_status()
        torrent_content = response.content
        info_hash = hashlib.sha1(torrent_content).hexdigest()
        magnet = f"magnet:?xt=urn:btih:{info_hash}"
        return magnet
    except Exception as e:
        logging.warning(f"Failed to convert {torrent_url} to magnet: {e}")
        return torrent_url  # Return torrent URL if conversion fails

def crawl_page(page_number, retries=0):
    """Crawl a single page"""
    try:
        # Construct page URL
        if page_number == 1:
            url = base_url
        else:
            url = f"{base_url}page/{page_number}/"
        
        logging.info(f"Scraping page {page_number}: {url}")
        response = session.get(url, headers=page_headers, timeout=TIMEOUT)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        torrent_rows = soup.select('tr[id^="tr-"]')
        if not torrent_rows:
            logging.warning(f"No torrent rows found on page {page_number}")
            return []
        
        results = []
        for row in torrent_rows:
            try:
                title_elem = row.select_one('a.torTopic.bold.tt-text')
                if not title_elem:
                    continue
                title = title_elem.get_text(strip=True)
                topic_url = urljoin(base_url, title_elem['href'])
                
                topic_id = get_topic_id(topic_url)
                download_url = get_download_url(topic_id)
                link = torrent_to_magnet(download_url) if download_url else ""
                
                publisher_elem = row.select_one('div.topicAuthor a.topicAuthor')
                publisher = publisher_elem.get_text(strip=True) if publisher_elem else "Unknown"
                
                results.append({
                    "Page": page_number,
                    "Title": title,
                    "URL": topic_url,
                    "Publisher": publisher,
                    "Link": link
                })
                
            except Exception as e:
                logging.error(f"Error processing row on page {page_number}: {e}")
                continue
        
        logging.info(f"Page {page_number}: Found {len(results)} items")
        return results
    
    except requests.RequestException as e:
        if retries < MAX_RETRIES:
            delay = RETRY_DELAY * (2 ** retries)
            logging.warning(f"Retry {retries + 1}/{MAX_RETRIES} for page {page_number} after {delay}s: {e}")
            time.sleep(delay)
            return crawl_page(page_number, retries + 1)
        logging.error(f"Failed to crawl page {page_number} after {MAX_RETRIES} attempts: {e}")
        return []

def crawl_pages(start_page, end_page):
    """Main crawling logic"""
    init_csv()
    total_records = 0
    pages = range(start_page, end_page - 1, -1)
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Submit all page crawling tasks
        future_to_page = {executor.submit(crawl_page, page): page for page in pages}
        
        # Process pages in descending order to maintain sequence
        pbar = tqdm(pages, desc="Crawling pages")
        for page_number in pbar:
            future = future_to_page.get(page_number)
            if future:
                try:
                    results = future.result()
                    if results:
                        with open(csv_file, mode='a', newline='', encoding='utf-8') as file:
                            writer = csv.writer(file)
                            for data in results:
                                writer.writerow([data["Page"], data["Title"], data["URL"], 
                                              data["Publisher"], data["Link"]])
                                total_records += 1
                        
                        if total_records >= COMMIT_INTERVAL:
                            git_commit(f"Update data for {total_records} records up to page {page_number}")
                            total_records = 0
                        
                except Exception as e:
                    logging.error(f"Error processing page {page_number}: {e}")
                
                time.sleep(random.uniform(0.5, 1.5))  # Delay between pages
    
    if total_records > 0:
        git_commit(f"Final update for remaining {total_records} records")

if __name__ == "__main__":
    # Initial request to establish session cookies
    try:
        session.get("https://pornotorrent.top/", headers=page_headers, timeout=TIMEOUT)
        logging.info("Initialized session with homepage request")
    except requests.RequestException as e:
        logging.warning(f"Failed to initialize session: {e}")
    
    start_page = int(os.getenv("START_PAGE", 283))
    end_page = int(os.getenv("END_PAGE", 1))
    logging.info(f"Starting crawl from page {start_page} to {end_page} for forum {forum_id}")
    crawl_pages(start_page, end_page)
    logging.info(f"Data saved to {csv_file}")
    session.close()
