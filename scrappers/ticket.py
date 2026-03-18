import cloudscraper
import json 

# Utiliser cloudscraper pour contourner Cloudflare
scraper = cloudscraper.create_scraper()

base_url = "https://www.ticket.ma/api/list-events?type=last&limit=100&categoryID=0&page=1"
output_file = "tickets_events.json"



try:
    response = scraper.get(base_url, timeout=15)
    
    if response.status_code != 200:
        print(f"Error: API returned status {response.status_code}")
        exit(1)
    
    data = response.json()
    events = data.get('list', [])
    
    if not events:
        print("No events found")
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
            "place": {k: v for k, v in place.items() if v is not None}
            
        }
        all_events.append(clean_event)
    
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(all_events, f, indent=2, ensure_ascii=False)
    
    print(f"✓ Scraped {len(all_events)} events")
    print(f"✓ Saved to: {output_file}")
    
except Exception as e:
    print(f"Error: {e}")
    exit(1)
