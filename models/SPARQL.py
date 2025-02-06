from rdflib import Graph, URIRef
import logging
import requests
import yaml
import json
import re

# Load Configuration from config.yaml
CONFIG_FILE = "llm_config.yaml"

try:
    with open(CONFIG_FILE, "r") as file:
        config = yaml.safe_load(file)
except FileNotFoundError:
    raise RuntimeError(f"Configuration file '{CONFIG_FILE}' not found.")
except yaml.YAMLError as e:
    raise RuntimeError(f"Error parsing '{CONFIG_FILE}': {e}")

# Configure logging
logging.basicConfig(
    level=config.get("logging", {}).get("level", "DEBUG"),
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# LLM Configuration
LLM_MODEL_NAME = config["llm"]["model_name"]
LLM_TEMPERATURE = config["llm"]["temperature"]
LLM_MAX_TOKENS = config["llm"]["max_tokens"]
LLM_API_URL = config["llm"]["api_url"]

# Knowledge Graph File
RDF_FILE = "knowledge_graph.ttl"

# SPARQL Query Template
SPARQL_TEMPLATE = """
PREFIX dcat: <http://www.w3.org/ns/dcat#>
PREFIX dcterms: <http://purl.org/dc/terms/>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>

SELECT DISTINCT ?dataset ?title ?description ?url ?format ?publisher
WHERE {
  ?dataset a dcat:Dataset .
  OPTIONAL { ?dataset dcterms:title ?title . }
  OPTIONAL { ?dataset dcterms:description ?description . }
  OPTIONAL { ?dataset dcat:distribution ?distribution .
             ?distribution dcat:mediaType ?format ;
                           dcat:downloadURL ?url . }
  OPTIONAL { ?dataset dcterms:publisher ?publisher . }
  {filters}
}
"""

def generate_sparql_query_with_llm(user_query):
    """
    Generate SPARQL query using the LLM model specified in the config.
    """
    prompt = (
        f"### Task\n"
        f"You are an assistant that converts natural language queries into SPARQL queries for a knowledge graph. "
        f"The knowledge graph uses the following structure:\n"
        f"- dcat:Dataset for datasets.\n"
        f"- dcterms:title and dcterms:description for dataset titles and descriptions.\n"
        f"- dcat:distribution for dataset distributions, with dcat:mediaType indicating file format (e.g., PDF, CSV) and dcat:downloadURL for URLs.\n"
        f"- dcterms:publisher for the dataset publisher.\n"
        f"- dcat:keyword for dataset tags.\n"
        f"- dcat:theme for dataset groups.\n\n"
        f"### SPARQL Template\n"
        f"{SPARQL_TEMPLATE}\n\n"
        f"### Example\n"
        f"User Query: 'Show me datasets about air pollution.'\n"
        f"SPARQL Query:\n"
        f"PREFIX dcat: <http://www.w3.org/ns/dcat#>\n"
        f"PREFIX dcterms: <http://purl.org/dc/terms/>\n\n"
        f"SELECT DISTINCT ?dataset ?title ?description ?url ?format ?publisher\n"
        f"WHERE {{\n"
        f"  ?dataset a dcat:Dataset .\n"
        f"  OPTIONAL {{ ?dataset dcterms:title ?title . }}\n"
        f"  OPTIONAL {{ ?dataset dcterms:description ?description . }}\n"
        f"  OPTIONAL {{ ?dataset dcat:distribution ?distribution .\n"
        f"             ?distribution dcat:mediaType ?format ;\n"
        f"                           dcat:downloadURL ?url . }}\n"
        f"  OPTIONAL {{ ?dataset dcterms:publisher ?publisher . }}\n"
        f"  OPTIONAL {{ ?dataset dcat:keyword ?keyword . }}\n"
        f"  OPTIONAL {{ ?dataset dcat:theme ?theme . }}\n"
        f"  FILTER (\n"
        f"    CONTAINS(LCASE(STR(?title)), \"air pollution\") ||\n"
        f"    CONTAINS(LCASE(STR(?description)), \"air pollution\") ||\n"
        f"    CONTAINS(LCASE(STR(?keyword)), \"air pollution\") ||\n"
        f"    CONTAINS(LCASE(STR(?theme)), \"air pollution\") ||\n"
        f"    CONTAINS(LCASE(STR(?url)), \"air pollution\")\n"
        f"  )\n"
       # f"  FILTER (LCASE(?format) = \"pdf\")\n"
        f"}}\n\n"
        f"### User Query\n{user_query}\n\n"
        f"### SPARQL Query"
    )

    try:
        payload = {
            "model": LLM_MODEL_NAME,
            "prompt": prompt,
            "temperature": LLM_TEMPERATURE,
            "max_tokens": LLM_MAX_TOKENS,
        }
        headers = {"Content-Type": "application/json"}

        response = requests.post(LLM_API_URL, headers=headers, json=payload, stream=True)
        response.raise_for_status()

        sparql_query = ""
        for line in response.iter_lines():
            if line:
                try:
                    chunk = json.loads(line)
                    sparql_query += chunk.get("response", "")
                except json.JSONDecodeError:
                    continue

        # Extract SPARQL query from the response
        sparql_query = sparql_query.strip()
        match = re.search(r"PREFIX.*WHERE\s*\{.*\}", sparql_query, re.DOTALL)
        if match:
            sparql_query = match.group(0)
        else:
            logger.error("Failed to extract SPARQL query from the response.")
            return None

        logger.info(f"Generated SPARQL Query:\n{sparql_query}")
        return sparql_query
    except requests.exceptions.RequestException as e:
        logger.error(f"Error communicating with LLM API: {e}")
        return None


def query_knowledge_graph(query_string):
    """
    Query the RDF knowledge graph using the generated SPARQL query.
    """
    g = Graph()
    try:
        g.parse(RDF_FILE, format="turtle")
        logger.info(f"Graph contains {len(g)} triples.")
    except FileNotFoundError:
        logger.error(f"RDF file '{RDF_FILE}' not found.")
        return []

    try:
        logger.debug(f"Executing SPARQL Query:\n{query_string}")
        results = g.query(query_string)
        output = []
        for row in results:
            # Extract the publisher URI and simplify it
            publisher_uri = str(row.get("publisher", "N/A"))
            if publisher_uri.startswith("http://yourprojectname.org/publisher/"):
                publisher_name = publisher_uri.split("/")[-1].replace("_", " ")
            else:
                publisher_name = publisher_uri

            dataset_info = {
                "dataset": str(row.get("dataset", "N/A")),
                "title": str(row.get("title", "N/A")),
                "description": str(row.get("description", "N/A")),
                "format": str(row.get("format", "N/A")),
                "url": str(row.get("url", "N/A")),
                "publisher": publisher_name,
            }
            output.append(dataset_info)
        return output
    except Exception as e:
        logger.error(f"Error executing SPARQL query: {e}")
        return []



def main():
    logger.info("Welcome to the Dataset Retrieval Assistant!")
    user_query = input("Enter your query (e.g., 'Show me datasets about air pollution in PDF'): ").strip()

    try:
        sparql_query = generate_sparql_query_with_llm(user_query)
        if sparql_query:
            logger.info(f"Generated SPARQL Query:\n{sparql_query}")
            results = query_knowledge_graph(sparql_query)

            if results:
                print("\nRelevant Datasets Found:\n")
                dataset_urls = []  # List to store dataset URLs

                for idx, result in enumerate(results, start=1):
                    print(f"Dataset {idx}:")
                    print(f"Title: {result.get('title', 'N/A')}")
                    print(f"Description: {result.get('description', 'N/A')}")
                    print(f"Format: {result.get('format', 'N/A')}")
                    print(f"Publisher: {result.get('publisher', 'N/A')}")
                    print(f"URL: {result.get('url', 'N/A')}")
                    print()

                    # Add URL to the list if it exists
                    dataset_url = result.get("url", "N/A")
                    if dataset_url and dataset_url != "N/A":
                        dataset_urls.append(dataset_url)

                # Save URLs to a file for future use
                if dataset_urls:
                    with open("dataset_urls.txt", "w") as url_file:
                        for url in dataset_urls:
                            url_file.write(url + "\n")
                    logger.info(f"Dataset URLs saved to 'dataset_urls.txt'.")

                    # Optionally call the exploration script
                    #from explore_datasets import handle_datasets
                    #handle_datasets(results)

                else:
                    print("No dataset URLs available for downloading.")
            else:
                print("\nNo relevant datasets found.")
        else:
            print("Failed to generate a SPARQL query. Please try again.")
    except Exception as e:
        logger.error(f"An error occurred during query processing: {e}")
        print("An unexpected error occurred. Please try again.")


# Make functions available for external use but prevent automatic execution
if __name__ == "__main__":
    main()