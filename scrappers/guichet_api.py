import requests as req
import json
import time

base_url="https://apiv2.guichet.com/v1/ticketing/events?limit=30"
all_events=[]
page=1



while True:
    url=f"{base_url}&page={page}"
    retries=0
    max_retries=3
    
    while retries < max_retries:
        try:
            response=req.get(url, timeout=15)
            data=response.json()
            events=data.get('events', [])
            
            if not events:
                print(f"No more events at page {page} - Stopping")
                break
            
            for event in events:
                clean_event={
                    "title": event.get('title'),
                    "description": event.get('description'),
                    "price": event.get('price'),
                    "startAt": event.get('startAt'),
                    "cover": event.get('cover'),
                    "city": event.get('city', {}).get('name') if event.get('city') else None,
                    "place": event.get('place', {}).get('name') if event.get('place') else None,
                    "category": event.get('category', {}).get('title') if event.get('category') else None,
                    "producer": event.get('producer', {}).get('title') if event.get('producer') else None,
                }
                all_events.append(clean_event)
            
            page+=1
            print(f"Scraped page {page-1} ({len(events)} events, total: {len(all_events)})")
            time.sleep(1)
            break
            
        except Exception as e:
            retries+=1
            print(f"Error on page {page} (retry {retries}/{max_retries}): {str(e)[:80]}")
            if retries < max_retries:
                time.sleep(5 * retries)  
            else:
                print("Max retries exceeded")
                break
    
    if not events or retries == max_retries:
        break



with open('guichet_events.json', 'w', encoding='utf-8') as f:
    json.dump(all_events, f, indent=2, ensure_ascii=False)


