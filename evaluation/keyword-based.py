import requests
import json
import time

# API details
API_KEY = "c535ba42-c70c-4cf0-826c-6a93bc6b1f2c"
BASE_URL = "https://data.london.gov.uk/api/3/action/package_search"

# Updated keyword queries (matching real formats)
keyword_queries = [
    "population growth",
    "crime rate by borough",
    "air pollution levels",
    "public transport usage",
    "average house prices",
    "unemployment statistics",
    "NHS waiting times",
    "energy consumption trends",
    "green space data",
    "business startup rates",
    "ethnic diversity",
    "road traffic accidents",
    "homelessness numbers",
    "broadband coverage",
    "waste recycling rates",
    "tourist visits",
    "school performance data",
    "CO2 emissions",
    "traffic congestion",
    "obesity rates"
]



import urllib.parse

def fetch_datasets(query):
    """Fetch datasets from the London Data API based on keyword query."""
    params = {
        "q": query,
        "rows": 50,  # Fetch more results
        "sort": "score desc, metadata_modified desc"  # Prioritize relevance & latest updates
    }
    headers = {"Authorization": API_KEY}

    try:
        response = requests.get(BASE_URL, params=params, headers=headers)
        response.raise_for_status()
        data = response.json()

        datasets = []
        if "result" in data["result"]:
            for dataset in data["result"]["result"]:
                dataset_name = dataset["name"]  # Dataset identifier for the correct URL
                dataset_id = dataset["id"]

                dataset_entry = {
                    "title": dataset["title"],
                    "dataset_page": f"https://data.london.gov.uk/dataset/{dataset_name}",
                    "resources": []
                }

                for resource in dataset.get("resources", []):
                    resource_id = resource.get("id", "")
                    resource_filename = resource.get("name", "")

                    # Encode filename properly to avoid spaces and special characters
                    encoded_filename = urllib.parse.quote(resource_filename)

                    # Construct the correct URL
                    correct_url = f"https://data.london.gov.uk/download/{dataset_name}/{resource_id}/{encoded_filename}"

                    dataset_entry["resources"].append({
                        "name": resource_filename,
                        "url": correct_url,
                        "format": resource.get("format", ""),
                    })

                datasets.append(dataset_entry)

            return datasets
        else:
            return []

    except requests.exceptions.RequestException as e:
        print(f"Error fetching data for query '{query}': {e}")
        return []


# Run the search
evaluation_dataset = []

for query in keyword_queries:
    print(f"Fetching results for: {query}")
    datasets = fetch_datasets(query)
    evaluation_dataset.append({"keyword_search": query, "retrieved_datasets": datasets})
    time.sleep(1)  # Delay to avoid API rate limits

# Save results
output_filename = "evaluation_dataset.json"
with open(output_filename, "w", encoding="utf-8") as f:
    json.dump(evaluation_dataset, f, indent=4)

print(f"✅ Evaluation dataset saved as {output_filename}") 