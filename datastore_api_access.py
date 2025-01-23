import requests
import json
import logging
import time
import re
from html import unescape

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

API_KEY = "c535ba42-c70c-4cf0-826c-6a93bc6b1f2c"
BASE_URL = "https://data.london.gov.uk/api/3/action/"

def fetch_dataset_list():
    """Fetch the list of all dataset identifiers."""
    try:
        response = requests.get(f"{BASE_URL}package_list")
        response.raise_for_status()
        dataset_list = response.json().get("result", [])
        logger.info(f"Fetched {len(dataset_list)} dataset identifiers.")
        return dataset_list
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching dataset list: {e}")
        return []

def fetch_metadata(dataset_id):
    """Fetch metadata for a single dataset."""
    try:
        response = requests.get(f"{BASE_URL}package_show", params={"id": dataset_id})
        response.raise_for_status()
        return response.json().get("result", {})
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching metadata for dataset {dataset_id}: {e}")
        return {}

def clean_html(html):
    """Remove HTML tags and decode HTML entities."""
    text = re.sub(r'<[^>]+>', '', html)  # Remove HTML tags
    return unescape(text.strip())  # Decode HTML entities and strip whitespace

def preprocess_metadata(metadata):
    """Preprocess metadata to clean and normalize fields."""
    if not metadata:
        return {}

    return {
        "id": metadata.get("id", "unknown"),
        "title": metadata.get("title", "Unnamed Dataset"),
        "summary": clean_html(metadata.get("notes", "No summary available")),
        "publisher": metadata.get("organization", {}).get("title", "Unknown Publisher"),
        "tags": [tag.get("name", "unknown") for tag in metadata.get("tags", [])],
        "groups": [group.get("title", "unknown") for group in metadata.get("groups", [])],
        "metadata_created": metadata.get("metadata_created", "Unknown Date"),
        "metadata_modified": metadata.get("metadata_modified", "Unknown Date"),
        "resources": [
            {
                "id": resource.get("id", "unknown"),
                "url": resource.get("url", "unknown"),
                "format": resource.get("format", "unknown"),
            }
            for resource in metadata.get("resources", [])
        ],
    }

def fetch_and_save_metadata(output_file="datasets.json"):
    """Fetch metadata for all datasets and save it as a JSON file."""
    dataset_list = fetch_dataset_list()
    if not dataset_list:
        logger.error("No datasets found. Exiting.")
        return

    all_metadata = []
    for idx, dataset_id in enumerate(dataset_list):
        logger.info(f"Fetching metadata for dataset {idx + 1}/{len(dataset_list)}: {dataset_id}")
        metadata = fetch_metadata(dataset_id)
        if metadata:
            processed_metadata = preprocess_metadata(metadata)
            all_metadata.append(processed_metadata)
        time.sleep(0.1)  # Delay to avoid rate limiting

    # Save all metadata to a JSON file
    try:
        with open(output_file, "w") as f:
            json.dump(all_metadata, f, indent=4)
        logger.info(f"Metadata saved to '{output_file}'")
    except Exception as e:
        logger.error(f"Error saving metadata to file: {e}")

if __name__ == "__main__":
    fetch_and_save_metadata()

