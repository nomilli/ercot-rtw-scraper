import requests
import pandas as pd
from bs4 import BeautifulSoup
import time
from datetime import datetime
import csv
import io

url = "https://www.ercot.com/content/cdr/html/hb_lz.html"
csv_filename = "ercot_lmp_filtered_log.csv"

# Columns to keep (excluding the two 5 Min Change columns)
columns = [
    "LMP", 
    "SPP"
]

headers = ["Date", "Time"]

headers.extend([f"HB_HOUSTON_{col}" for col in columns])
headers.extend([f"LZ_HOUSTON_{col}" for col in columns])

# Initialize CSV with headers if not exists
try:
    with open(csv_filename, "x", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(headers)
except FileExistsError:
    pass

last_update = None

def parse_update_time(update_line):
    # Example input: 'Last Updated:  Oct 21, 2025 23:15:10'
    if update_line:
        # Remove 'Last Updated:' and any leading whitespace
        timestamp_str = update_line[0].replace('Last Updated:', '').strip()
        # Parse according to the format like 'Oct 21, 2025 23:15:10'
        dt = datetime.strptime(timestamp_str, '%b %d, %Y %H:%M:%S')
        return dt
    else:
        return datetime.now()

def fetch_lmp_table():
    response = requests.get(url)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")

    table = soup.find("table")
    df = pd.read_html(io.StringIO(str(table)))[0]

    # Filter rows
    df_filtered = df[df[0].isin(['Settlement Point', 'HB_HOUSTON', 'LZ_HOUSTON'])]

    # Select only required columns
    df_filtered = df_filtered[[0,1,3]]

    # Extract last updated time
    update_text = soup.get_text()
    update_line = [line for line in update_text.splitlines() if "Last Updated" in line]
    update_time = parse_update_time(update_line)

    return df_filtered, update_time

def record_lmp(df, update_time):
	date_str = update_time.strftime('%Y-%m-%d')
	time_str = update_time.strftime('%H:%M:%S')
    # Assuming df contains exactly rows for HB_HOUSTON and LZ_HOUSTON
    # Extract data by settlement point
	hb = df[df[0] == "HB_HOUSTON"].iloc[0]
	lz = df[df[0] == "LZ_HOUSTON"].iloc[0]

    # Construct one row: timestamp + HB columns (except Settlement Point) + LZ columns (except Settlement Point)
	row = [date_str,time_str]
	# Add HB values except 'Settlement Point'
	row.extend(hb.drop(0).tolist())
    # Add LZ values except 'Settlement Point'
	row.extend(lz.drop(0).tolist())
	
	with open(csv_filename, "a", newline="") as f:
		writer = csv.writer(f)
		writer.writerow(row)

sleep_time = 10
print("Monitoring ERCOT LMP page. Press Ctrl+C to stop.")

while True:
    try:
        df, update_time = fetch_lmp_table()
        if update_time != last_update:
            print(f"New data detected at {update_time}, recording {len(df)} rows.")
            record_lmp(df, update_time)
            last_update = update_time
            sleep_time = 290 #4'50" updates expected every 5 minutes
        else:
            print(f"No change detected at {datetime.now().isoformat()}.")
            sleep_time = 5
    except Exception as e:
        print(f"Error fetching or processing data: {e}")
    time.sleep(sleep_time)