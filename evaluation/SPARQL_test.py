import json
import logging
from models.SPARQL import generate_sparql_query_with_llm, query_knowledge_graph

# Load evaluation queries
EVALUATION_FILE = "evaluation/keywordsearch.json"
OUTPUT_FILE = "evaluation/SPARQL_results.json"

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

try:
    with open(EVALUATION_FILE, "r", encoding="utf-8") as f:
        evaluation_data = json.load(f)
except FileNotFoundError:
    logger.error(f"Evaluation dataset '{EVALUATION_FILE}' not found.")
    exit(1)

# Function to convert short keyword queries into natural language
def rephrase_query(keyword_query):
    return f"Can you show me datasets about {keyword_query}?"

# Store chatbot search results
chatbot_results = []

logger.info("Starting chatbot dataset search evaluation...")

for item in evaluation_data:
    keyword_query = item["keyword_search"]
    natural_query = rephrase_query(keyword_query)  # Convert to natural language

    logger.info(f"Processing query: {natural_query}")

    # Generate SPARQL Query using the chatbot (LLM)
    sparql_query = generate_sparql_query_with_llm(natural_query)
    
    if sparql_query:
        # Execute SPARQL Query on the Knowledge Graph
        results = query_knowledge_graph(sparql_query)

        # Ensure identical structure to evaluation_dataset.json
        dataset_dict = {}  # Dictionary to merge resources under dataset titles

        for dataset in results:
            dataset_title = dataset.get("title", "Unknown Title")
            dataset_description = dataset.get("description", "No description available.")
            dataset_url = dataset.get("url", "")
            dataset_format = dataset.get("format", "Unknown Format")
            dataset_name = dataset_url.split("/")[-1] if dataset_url else "Unknown Name"  # Extract file name from URL

            if dataset_title not in dataset_dict:
                dataset_dict[dataset_title] = {
                    "title": dataset_title,
                    "description": dataset_description,  # Include descriptions
                    "resources": []
                }

            dataset_dict[dataset_title]["resources"].append({
                "name": dataset_name,
                "url": dataset_url,
                "format": dataset_format
            })

        # Convert dict back to list
        formatted_results = list(dataset_dict.values())

        chatbot_results.append({
            "keyword_search": keyword_query,
            "natural_query": natural_query,
            "retrieved_datasets": formatted_results
        })

        logger.info(f"Retrieved {len(results)} datasets for query: '{natural_query}'")

    else:
        logger.warning(f"Failed to generate SPARQL for query: '{natural_query}'")
        chatbot_results.append({
            "keyword_search": keyword_query,
            "natural_query": natural_query,
            "retrieved_datasets": []
        })

# Save chatbot results to JSON
with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
    json.dump(chatbot_results, f, indent=4, ensure_ascii=False)

logger.info(f"✅ Chatbot search results saved to '{OUTPUT_FILE}'")
