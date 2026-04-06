import cloudscraper
import json
from kafka import KafkaProducer

# Kafka producer
producer = KafkaProducer(
    bootstrap_servers='localhost:29092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

scraper = cloudscraper.create_scraper()

base_url = "https://www.ticket.ma/api/list-events?type=last&limit=100&categoryID=0&page=1"

try:
    response = scraper.get(base_url, timeout=15)
    
    if response.status_code != 200:
        print(f"Error: API returned status {response.status_code}")
        producer.close()
        exit(1)
    
    data = response.json()
    events = data.get('list', [])
    
    if not events:
        print("No events found")
        producer.close()
        exit(1)
    
    all_events = []
    for event in events:
        
        place = {
            "name": event.get("place", {}).get("name") if isinstance(event.get("place"), dict) else None,
            "theater_location": event.get("theater_location"),
            "country": event.get("country"),
            "city": event.get("city"),
            "address": event.get("address"),
        }
        
        clean_event = {
            "title": event.get('title'),
            "description": event.get('description'),
            "price": event.get('price'),
            "start_date": event.get('start_date'),
            "event_date": event.get('event_date'),
            "open_at": event.get('open_at'),
            "image": event.get('image'),
            "category": event.get('category'),
            "source": "ticket.ma",
            "place": {k: v for k, v in place.items() if v is not None}
        }
        all_events.append(clean_event)
        producer.send('eventsmorroco', value=clean_event, key=clean_event['title'].encode('utf-8'))
    
    print(json.dumps(all_events))
    producer.flush()
    producer.close()
    
except Exception as e:
    print(f"Error: {e}")
    producer.close()
    exit(1)
