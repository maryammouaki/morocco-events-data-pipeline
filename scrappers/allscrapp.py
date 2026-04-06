import json
import subprocess
import os
from datetime import datetime

class AllScraper:
    def __init__(self):
        self.script_dir = os.path.dirname(os.path.abspath(__file__))
        self.all_events = []
    
    def run_scraper(self, script_name):
        """Run scraper and capture JSON output"""
        print(f"\n>>> Running {script_name}...")
        try:
            result = subprocess.run(
                ['python', script_name],
                cwd=self.script_dir,
                capture_output=True,
                text=True
            )
            
            if result.returncode != 0:
                print(f" {script_name} error")
                return []
            
            # Extract JSON from output (last line should be JSON)
            lines = result.stdout.strip().split('\n')
            for line in reversed(lines):
                try:
                    events = json.loads(line)
                    if isinstance(events, list):
                        print(f"✓ {script_name} - Got {len(events)} events")
                        return events
                except:
                    pass
            
            print(f" No JSON found in output")
            return []
            
        except Exception as e:
            print(f"Error: {e}")
            return []
    
    def add_source(self, events, source_name):
        """Add source and timestamp to events"""
        for event in events:
            event['source'] = source_name
            event['scraped_at'] = datetime.now().isoformat()
        return events
    
    def save_combined_events(self):
        """Save all events to single file"""
        output_file = os.path.join(self.script_dir, 'all_events.json')
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(self.all_events, f, indent=2, ensure_ascii=False)
        print(f"\n✓ Saved {len(self.all_events)} events to all_events.json")
    
    def run_all(self):
        """Run all scrapers and combine output"""
        print("\n" + "="*50)
        print("ALL SCRAPER")
        print("="*50)
        
        # Run scrapers and capture output
        guichet_events = self.run_scraper('guichet_api.py')
        ticket_events = self.run_scraper('ticket.py')
        allevents = self.run_scraper('all_events.py')
        
        # Add source and combine
        self.all_events.extend(self.add_source(guichet_events, 'guichet'))
        self.all_events.extend(self.add_source(ticket_events, 'ticket.ma'))
        self.all_events.extend(self.add_source(allevents, 'allevents'))
        
        self.save_combined_events()
        
        print("\nSummary:")
        print(f"  Guichet:  {len(guichet_events)}")
        print(f"  Ticket.ma: {len(ticket_events)}")
        print(f"  AllEvents: {len(allevents)}")
        print(f"  Total: {len(self.all_events)}")

if __name__ == "__main__":
    scraper = AllScraper()
    scraper.run_all()
