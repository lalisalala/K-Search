import json
import logging
import models.faiss_search as faiss_search  # Import FAISS search module

# File paths
EVALUATION_FILE = "evaluation/keywordsearch.json"
OUTPUT_FILE = "evaluation/FAISS_results.json"

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

try:
    with open(EVALUATION_FILE, "r", encoding="utf-8") as f:
        evaluation_data = json.load(f)
except FileNotFoundError:
    logger.error(f"❌ Evaluation dataset '{EVALUATION_FILE}' not found.")
    exit(1)

# Store FAISS search results
faiss_results = []

logger.info("🔍 Starting FAISS dataset search evaluation...")

for item in evaluation_data:
    keyword_query = item["keyword_search"]

    logger.info(f"🔹 Processing query: {keyword_query}")

    # Perform FAISS similarity search
    retrieved_datasets = faiss_search.search_datasets(
        keyword_query, 
        similarity_threshold=0.9,  # Adjust if needed
        max_results=20 # Allows flexibility in the number of results
    )

    # Dictionary to merge resources under dataset titles
    dataset_dict = {}

    for dataset in retrieved_datasets:
        dataset_title = dataset.get("title", "Unknown Title")
        dataset_description = dataset.get("description", "No description available.")
        dataset_url = dataset.get("url", "")
        
        # Ensure resources are included
        resources = dataset.get("resources", [])

        if dataset_title not in dataset_dict:
            dataset_dict[dataset_title] = {
                "title": dataset_title,
                "description": dataset_description,  # Include descriptions
                "resources": []
            }

        # Append all resources (including URLs and formats)
        for res in resources:
            dataset_dict[dataset_title]["resources"].append({
                "name": res.get("name", "Unknown Name"),
                "url": res.get("url", dataset_url),  # If no specific resource URL, use dataset URL
                "format": res.get("format", "Unknown Format")
            })

    # Convert dict back to list
    formatted_results = list(dataset_dict.values())

    faiss_results.append({
        "keyword_search": keyword_query,
        "natural_query": f"Can you show me datasets about {keyword_query}?",
        "retrieved_datasets": formatted_results
    })

    logger.info(f"✅ Retrieved {len(retrieved_datasets)} datasets for query: '{keyword_query}'")

# Save FAISS search results to JSON
with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
    json.dump(faiss_results, f, indent=4, ensure_ascii=False)

logger.info(f"✅ FAISS search results saved to '{OUTPUT_FILE}'")
