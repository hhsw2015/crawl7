import csv
import os
import re

# Directory containing CSV files (adjust path as needed)
directory = "."

# Initialize lists for torrent and magnet links
torrent_list = []
magnet_list = []

# Get all CSV files
csv_files = [f for f in os.listdir(directory) if f.endswith(".csv")]


# Function to extract base number and optional suffix (e.g., 1677_7 -> (1677, 7))
def parse_filename(filename):
    match = re.match(r"(\d+)(?:_(\d+))?\.csv$", filename)
    if match:
        base = int(match.group(1))
        suffix = (
            int(match.group(2)) if match.group(2) else -1
        )  # Use -1 for files without suffix
        return base, suffix
    return float("inf"), -1  # Non-matching files go to the end


# Sort files: group by base number, then by suffix (-1 for no suffix, then 0, 1, 2, ...)
csv_files.sort(key=lambda f: parse_filename(f))

# Regex patterns
torrent_pattern = r"https://files\.cdntraffic\.top/PL/torrent/files/\d+\.torrent"
magnet_pattern = r"magnet:\?xt=urn:btih:[a-zA-Z0-9]+"

# Process each CSV file
for filename in csv_files:
    file_path = os.path.join(directory, filename)
    file_torrent_count = 0
    file_magnet_count = 0
    try:
        with open(file_path, "r", encoding="utf-8") as csv_file:
            reader = csv.reader(csv_file)
            for row in reader:
                # Each row is a list of strings; join to process as a single string
                line = " ".join(row)
                # Check for torrent URL
                torrent_match = re.search(torrent_pattern, line)
                if torrent_match:
                    torrent_list.append(torrent_match.group(0))
                    file_torrent_count += 1
                # Check for magnet link using regex
                magnet_match = re.search(magnet_pattern, line)
                if magnet_match:
                    magnet_list.append(magnet_match.group(0))
                    file_magnet_count += 1
        # Print counts for this file
        print(
            f"{filename}: {file_torrent_count} torrent links, {file_magnet_count} magnet links"
        )
    except Exception as e:
        print(f"Error reading {filename}: {e}")

# Append torrent links to torrent.txt
with open("torrent.txt", "a", encoding="utf-8") as f:
    for link in torrent_list:
        f.write(link + "\n")

# Append magnet links to magnet.txt
with open("magnet.txt", "a", encoding="utf-8") as f:
    for link in magnet_list:
        f.write(link + "\n")

# Print total counts
print(f"\nTotal: {len(torrent_list)} torrent links, {len(magnet_list)} magnet links")
