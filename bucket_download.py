import requests
import os
import json
import shutil
import gzip

def fetch_api_response(api_url):
    """Fetch the JSON response from the API."""
    response = requests.get(api_url)
    response.raise_for_status()  # Ensure the request was successful
    return response.json()

def download_file(url, local_directory, filename, json_file_name):
    """
    Download a .gz file, decompress it, and save it as a .json file.
    """
    os.makedirs(local_directory, exist_ok=True)  # Ensure directory exists

    # Paths for the compressed and decompressed files
    gz_file_path = os.path.join(local_directory, filename + ".gz")
    json_file_path = os.path.join(local_directory, json_file_name)

    try:
        # Download the .gz file
        print(f"Downloading: {url}")
        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            with open(gz_file_path, "wb") as f:
                shutil.copyfileobj(r.raw, f)

        # Decompress the .gz file to a .json file
        with gzip.open(gz_file_path, "rt", encoding="utf-8") as gz_file:
            with open(json_file_path, "w", encoding="utf-8") as json_file:
                shutil.copyfileobj(gz_file, json_file)

        # Optional: Remove the .gz file to save space
        os.remove(gz_file_path)
        print(f"Decompressed and saved JSON: {json_file_path}")

    except Exception as e:
        print(f"Failed to process {url}: {e}")


def process_country_urls(json_data, file_format, local_directory):
    """Process the JSON data to download files by format."""
    countries = json_data.get("countries", {})
    for country_code, formats in countries.items():
        if file_format in formats:
            file_info = formats[file_format]
            file_url = file_info["uri"]
            file_name = f"{country_code}_{file_format}.{file_url.split('.')[-2]}.{file_url.split('.')[-1]}"
            json_file_name = f"{country_code}_{file_format}.json"
            try:
                download_file(file_url, local_directory,file_name,json_file_name)
            except requests.exceptions.RequestException as e:
                print(f"Failed to download {file_url}: {e}")

# Main execution
if __name__ == "__main__":
    api_url = "https://app.ticketmaster.com/discovery-feed/v2/events?apikey=o3bK4TAxbI1ov8yhLCXp72bMkIpgezzQ"
  # Replace with your API URL
    file_format = "JSON"  # Specify desired format: JSON, CSV, or XML
    local_directory = "./downloads"  # Directory to save the downloaded files

    try:
        # Step 1: Fetch the JSON response
        print("Fetching API response...")
        json_data = fetch_api_response(api_url)
        
        # Step 2: Process the JSON data and download files
        print("Processing and downloading files...")
        process_country_urls(json_data, file_format, local_directory)
        print("All files downloaded successfully.")
    
    except requests.exceptions.RequestException as e:
        print(f"An error occurred while making the API request: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")

