import schedule
import time

def collect_data():
    # Your data collection code goes here
    print("Collecting data...")

# Schedule the data collection function to run every 5 minutes
schedule.every(5).minutes.do(collect_data)

while True:
    # Run pending scheduled jobs
    schedule.run_pending()
    time.sleep(1)  # Sleep for 1 second to avoid CPU-intensive loop
