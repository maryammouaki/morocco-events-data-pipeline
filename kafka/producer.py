import subprocess
import os

# Get the script directory
script_dir = os.path.dirname(os.path.abspath(__file__))
scrappers_dir = os.path.join(script_dir, '..', 'scrappers')

print("="*50)
print("RUNNING SCRAPERS - Publishing to Kafka")
print("="*50)

scrapers = ['guichet_api.py', 'ticket.py', 'all_events.py']

for scraper in scrapers:
    print(f"\n>>> Running {scraper}...")
    result = subprocess.run(
        ['python', scraper],
        cwd=scrappers_dir,
        capture_output=True,
        text=True
    )
    
    if result.returncode != 0:
        print(f"✗ {scraper} failed")
        if result.stderr:
            print(result.stderr)
    else:
        print(f"✓ {scraper} completed")

print("\n" + "="*50)
print("All scrapers completed!")
print("="*50)
