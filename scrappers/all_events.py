import requests
from bs4 import BeautifulSoup
import json
import time
import logging
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from kafka import KafkaProducer

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Kafka producer
producer = KafkaProducer(
    bootstrap_servers='localhost:29092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Configuration
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}
BASE_URL = 'https://allevents.in'

def create_session_with_retries(retries=3, backoff_factor=0.5):
    """Create a requests session with automatic retry logic"""
    session = requests.Session()
    retry_strategy = Retry(
        total=retries,
        backoff_factor=backoff_factor,
        status_forcelist=[429, 500, 502, 503, 504],
        method_whitelist=["GET"]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session

def get_event_details(url, session):
    """Fetch detailed event information"""
    try:
        response = session.get(url, headers=HEADERS, timeout=10)
        if response.status_code != 200:
            logger.warning(f"Failed to fetch event details from {url}: Status {response.status_code}")
            return None, None
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Try LD+JSON for description and category
        ld_json_scripts = soup.find_all('script', type='application/ld+json')
        description = ""
        category = ""
        
        for script in ld_json_scripts:
            try:
                data = json.loads(script.string)
                if isinstance(data, dict) and (data.get('@type') == 'Event' or data.get('@type') == 'MusicEvent'):
                    description = data.get('description', '')
                    category = data.get('keywords', '')
                    if not category:
                        tags = soup.select('.event-tags a')
                        if tags:
                            category = tags[0].get_text().strip()
                    break
            except Exception as e:
                logger.debug(f"Error parsing LD+JSON: {e}")
                continue
        
        if not description:
            desc_tag = soup.select_one('.event-description-html')
            if desc_tag:
                description = desc_tag.get_text().strip()
        
        return description, category
    except Exception as e:
        logger.error(f"Error fetching event details from {url}: {e}")
        return None, None

def get_events_from_listing(url, session, max_pages=None):
    """Scrape all pages from a listing URL with pagination"""
    all_events = []
    page = 1
    
    while True:
        try:
            if page == 1:
                paginated_url = url
            else:
                paginated_url = f"{url}?page={page}"
            
            logger.info(f"Fetching page {page}: {paginated_url}")
            response = session.get(paginated_url, headers=HEADERS, timeout=15)
            
            if response.status_code != 200:
                logger.warning(f"Failed to fetch {paginated_url}: Status {response.status_code}")
                break
            
            soup = BeautifulSoup(response.content, 'html.parser')
            cards = soup.select('.event-card')
            
            if not cards:
                logger.info(f"No more events found on page {page}")
                break
            
            logger.info(f"Found {len(cards)} events on page {page}")
            
            for card in cards:
                try:
                    title_tag = card.select_one('.title a')
                    if not title_tag:
                        continue
                    
                    title = title_tag.get('title') or title_tag.get_text().strip()
                    event_url = title_tag.get('href')
                    if event_url and not event_url.startswith('http'):
                        event_url = BASE_URL + event_url
                    
                    start_at = card.select_one('.date').get_text().strip() if card.select_one('.date') else ""
                    city = card.select_one('.location').get_text().strip() if card.select_one('.location') else ""
                    img_tag = card.select_one('img')
                    cover = ""
                    if img_tag:
                        cover = img_tag.get('data-src') or img_tag.get('src')
                    
                    all_events.append({
                        'title': title,
                        'url': event_url,
                        'startAt': start_at,
                        'city': city,
                        'cover': cover,
                        'source': "allevents.in"
                    })
                except Exception as e:
                    logger.error(f"Error parsing event card: {e}")
                    continue
            
            next_btn = soup.select_one('a.next') or soup.select_one('a[rel="next"]')
            if not next_btn:
                logger.info("No next page button found, pagination complete")
                break
            
            page += 1
            
            if max_pages and page > max_pages:
                logger.info(f"Reached maximum pages limit ({max_pages})")
                break
            
            time.sleep(1)
            
        except Exception as e:
            logger.error(f"Error fetching page {page}: {e}")
            break
    
    return all_events

def main():
    cities = ['casablanca', 'marrakesh', 'rabat', 'fez', 'agadir', 'tangier-tg']
    all_cleaned_events = []
    seen_urls = set()
    
    session = create_session_with_retries()
    
    try:
        for city in cities:
            logger.info(f"Starting scraping for {city}...")
            url = f"{BASE_URL}/{city}/all"
            
            listing_events = get_events_from_listing(url, session, max_pages=None)
            logger.info(f"Found {len(listing_events)} events in listing for {city}")
            
            for idx, event in enumerate(listing_events, 1):
                if event['url'] and event['url'] not in seen_urls:
                    logger.info(f"  [{idx}/{len(listing_events)}] Fetching details for: {event['title']}")
                    desc, cat = get_event_details(event['url'], session)
                    event['description'] = desc or ""
                    event['category'] = cat or ""
                    all_cleaned_events.append(event)
                    producer.send('eventsmorroco', value=event, key=event['url'].encode('utf-8'))
                    seen_urls.add(event['url'])
                    time.sleep(0.5)
                else:
                    logger.debug(f"  Skipping duplicate event: {event.get('title')}")
            
            logger.info(f"Completed {city}. Total events so far: {len(all_cleaned_events)}")
            time.sleep(2)
        
        print(json.dumps(all_cleaned_events))
        producer.flush()
        
    except Exception as e:
        logger.error(f"Fatal error in main: {e}")
    finally:
        producer.close()
        session.close()
        logger.info("Session closed")

if __name__ == "__main__":
    main()
