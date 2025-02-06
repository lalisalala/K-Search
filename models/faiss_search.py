import os
import requests
import pandas as pd
import json
import faiss
from sentence_transformers import SentenceTransformer

# File paths
METADATA_FILE = "london_datasets_metadata.json"
CSV_FILE = "london_datasets_metadata.csv"
FAISS_INDEX_FILE = "faiss_index.bin"

# API Details
API_KEY = "c535ba42-c70c-4cf0-826c-6a93bc6b1f2c"
BASE_URL = "https://data.london.gov.uk/api/3/action/package_list"
DATASET_DETAILS_URL = "https://data.london.gov.uk/api/3/action/package_show"

# Check if metadata file already exists
if os.path.exists(METADATA_FILE):
    print("✅ Metadata file found. Skipping API fetch and loading existing metadata...")
    with open(METADATA_FILE, "r", encoding="utf-8") as json_file:
        metadata_list = json.load(json_file)
else:
    print("🔍 Metadata file not found. Fetching data from API...")
    
    # Fetch dataset list
    headers = {"Authorization": API_KEY}
    response = requests.get(BASE_URL, headers=headers)
    dataset_list = response.json().get("result", [])

    metadata_list = []
    
    print(f"Fetching metadata for {len(dataset_list)} datasets...")

    # Retrieve metadata for each dataset
    for dataset_name in dataset_list:
        try:
            dataset_url = f"{DATASET_DETAILS_URL}?id={dataset_name}"
            response = requests.get(dataset_url, headers=headers)
            data = response.json().get("result", {})

            # Extract resources including format
            resources = [
                {
                    "name": res.get("name", "Unknown Name"),
                    "url": res.get("url", ""),
                    "format": res.get("format", "Unknown Format")  # Ensure format is captured
                }
                for res in data.get("resources", [])  # Check if resources exist
            ]

            metadata = {
                "id": data.get("id"),
                "title": data.get("title"),
                "description": data.get("notes"),
                "tags": [tag["name"] for tag in data.get("tags", [])],
                "groups": [group["name"] for group in data.get("groups", [])],
                "license": data.get("license_title"),
                "organization": data.get("organization", {}).get("title"),
                "url": f"https://data.london.gov.uk/dataset/{data.get('name')}",
                "resources": resources  # Store resources with format!
            }
            metadata_list.append(metadata)

        except Exception as e:
            print(f"Error fetching dataset {dataset_name}: {e}")

    # Save metadata to JSON and CSV
    with open(METADATA_FILE, "w", encoding="utf-8") as json_file:
        json.dump(metadata_list, json_file, indent=4)

    df = pd.DataFrame(metadata_list)
    df.to_csv(CSV_FILE, index=False)

    print("✅ Metadata saved successfully!")

# Initialize FAISS and Sentence Transformer for similarity search
model = SentenceTransformer("all-MiniLM-L6-v2")

# Convert dataset descriptions to embeddings
descriptions = [dataset["description"] if dataset["description"] else "" for dataset in metadata_list]
embeddings = model.encode(descriptions)

# Check if FAISS index already exists
if os.path.exists(FAISS_INDEX_FILE):
    print("✅ FAISS index found. Loading existing index...")
    index = faiss.read_index(FAISS_INDEX_FILE)
else:
    print("⚡ FAISS index not found. Creating a new FAISS index...")
    d = embeddings.shape[1]  # Dimension of embeddings
    index = faiss.IndexFlatL2(d)
    index.add(embeddings)
    faiss.write_index(index, FAISS_INDEX_FILE)
    print("✅ FAISS index created and saved!")

# 🔍 Function to search datasets with a similarity threshold
def search_datasets(query, similarity_threshold=0.95, max_results=20):
    query_embedding = model.encode([query])
    distances, indices = index.search(query_embedding, max_results)  # Search for many results

    results = []
    for score, idx in zip(distances[0], indices[0]):
        if idx < len(metadata_list) and score >= similarity_threshold:  # Apply threshold
            dataset = metadata_list[idx]  # Get dataset entry
            
            # Extract resources including format
            resources = [
                {
                    "name": res.get("name", "Unknown Name"),
                    "url": res.get("url", ""),
                    "format": res.get("format", "Unknown Format")  # Ensure format is included!
                }
                for res in dataset.get("resources", [])  # Ensure resources exist
            ]
            
            results.append({
                "title": dataset["title"],
                "url": dataset["url"],
                "resources": resources  # Now includes format!
            })

    return results

# 🟢 Example usage
query = "Air pollution in London"
search_results = search_datasets(query)

print("\n🔎 Top matching datasets:")
for i, result in enumerate(search_results):
    print(f"{i+1}. {result['title']} - {result['url']}")
    for res in result["resources"]:
        print(f"   📄 Format: {res['format']} | 🔗 {res['url']}")
