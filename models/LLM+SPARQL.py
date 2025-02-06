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

SELECT DISTINCT ?dataset ?title ?description ?url ?format ?publisher ?created ?modified
WHERE {
  ?dataset a dcat:Dataset .
  OPTIONAL { ?dataset dcterms:title ?title . }
  OPTIONAL { ?dataset dcterms:description ?description . }
  OPTIONAL { ?dataset dcat:keyword ?keyword . }
  OPTIONAL { ?dataset dcat:theme ?theme . }
  OPTIONAL { ?dataset dcat:distribution ?distribution .
             ?distribution dcat:mediaType ?format ;
                           dcat:downloadURL ?url . }
  OPTIONAL { ?dataset dcterms:publisher ?publisher . }
  OPTIONAL { ?dataset dcterms:created ?created . }
  OPTIONAL { ?dataset dcterms:modified ?modified . }

  FILTER (
    REGEX(LCASE(STR(?title)), "{keywords}", "i") ||
    REGEX(LCASE(STR(?description)), "{keywords}", "i") ||
    REGEX(LCASE(STR(?keyword)), "{keywords}", "i") ||
    REGEX(LCASE(STR(?theme)), "{keywords}", "i")
  )
}
ORDER BY DESC(
  (IF(CONTAINS(LCASE(STR(?title)), "{keywords}"), 3, 0) +
   IF(CONTAINS(LCASE(STR(?description)), "{keywords}"), 2, 0) +
   IF(CONTAINS(LCASE(STR(?keyword)), "{keywords}"), 1, 0))
)
}
"""

def generate_sparql_query_with_llm(user_query):
    """
    Generate SPARQL query using the LLM model specified in the config.
    """
    prompt = f"""
    ### Task
    You are an assistant that converts natural language queries into **SPARQL queries** for a knowledge graph.
    The knowledge graph contains metadata about datasets, including:

    - **Titles** (`dcterms:title`)
    - **Descriptions** (`dcterms:description`)
    - **Keywords** (`dcat:keyword`)
    - **Themes** (`dcat:theme`)
    - **Download URLs** (`dcat:downloadURL`)
    - **Publishers** (`dcterms:publisher`)
    - **Dataset creation & modification dates** (`dcterms:created`, `dcterms:modified`)

    The SPARQL query should:
    - **Match datasets based on keywords in the title, description, keyword, and theme.**
    - **Use REGEX matching (`REGEX(LCASE(...))`) for flexible text search.**
    - **Order results by relevance using weighted ranking.**
    - **Include dataset creation/modification dates if available.**
    
    ### SPARQL Query Template
    ```sparql
    PREFIX dcat: <http://www.w3.org/ns/dcat#>
    PREFIX dcterms: <http://purl.org/dc/terms/>
    PREFIX skos: <http://www.w3.org/2004/02/skos/core#>

    SELECT DISTINCT ?dataset ?title ?description ?url ?format ?publisher ?created ?modified
    WHERE {{
      ?dataset a dcat:Dataset .
      OPTIONAL {{ ?dataset dcterms:title ?title . }}
      OPTIONAL {{ ?dataset dcterms:description ?description . }}
      OPTIONAL {{ ?dataset dcat:keyword ?keyword . }}
      OPTIONAL {{ ?dataset dcat:theme ?theme . }}
      OPTIONAL {{ ?dataset dcat:distribution ?distribution .
                 ?distribution dcat:mediaType ?format ;
                               dcat:downloadURL ?url . }}
      OPTIONAL {{ ?dataset dcterms:publisher ?publisher . }}
      OPTIONAL {{ ?dataset dcterms:created ?created . }}
      OPTIONAL {{ ?dataset dcterms:modified ?modified . }}

      FILTER (
        REGEX(LCASE(STR(?title)), "{user_query}", "i") ||
        REGEX(LCASE(STR(?description)), "{user_query}", "i") ||
        REGEX(LCASE(STR(?keyword)), "{user_query}", "i") ||
        REGEX(LCASE(STR(?theme)), "{user_query}", "i")
      )
    }}
    ORDER BY DESC(
      (IF(CONTAINS(LCASE(STR(?title)), "{user_query}"), 3, 0) +
       IF(CONTAINS(LCASE(STR(?description)), "{user_query}"), 2, 0) +
       IF(CONTAINS(LCASE(STR(?keyword)), "{user_query}"), 1, 0))
    )
    ```

    ### Example
    **User Query:** `"Show me datasets about air pollution"`
    
    **SPARQL Query Output:**
    ```sparql
    PREFIX dcat: <http://www.w3.org/ns/dcat#>
    PREFIX dcterms: <http://purl.org/dc/terms/>

    SELECT DISTINCT ?dataset ?title ?description ?url ?format ?publisher ?created ?modified
    WHERE {{
      ?dataset a dcat:Dataset .
      OPTIONAL {{ ?dataset dcterms:title ?title . }}
      OPTIONAL {{ ?dataset dcterms:description ?description . }}
      OPTIONAL {{ ?dataset dcat:keyword ?keyword . }}
      OPTIONAL {{ ?dataset dcat:theme ?theme . }}
      OPTIONAL {{ ?dataset dcat:distribution ?distribution .
                 ?distribution dcat:mediaType ?format ;
                               dcat:downloadURL ?url . }}
      OPTIONAL {{ ?dataset dcterms:publisher ?publisher . }}
      OPTIONAL {{ ?dataset dcterms:created ?created . }}
      OPTIONAL {{ ?dataset dcterms:modified ?modified . }}

      FILTER (
        REGEX(LCASE(STR(?title)), "air pollution", "i") ||
        REGEX(LCASE(STR(?description)), "air pollution", "i") ||
        REGEX(LCASE(STR(?keyword)), "air pollution", "i") ||
        REGEX(LCASE(STR(?theme)), "air pollution", "i")
      )
    }}
    ORDER BY DESC(
      (IF(CONTAINS(LCASE(STR(?title)), "air pollution"), 3, 0) +
       IF(CONTAINS(LCASE(STR(?description)), "air pollution"), 2, 0) +
       IF(CONTAINS(LCASE(STR(?keyword)), "air pollution"), 1, 0))
    )
    ```

    ### User Query
    "{user_query}"

    ### SPARQL Query:
    """

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



def refine_results_with_llm(user_query, sparql_results):
    """
    Uses the same LLM to refine and rank SPARQL query results.
    """
    # Convert SPARQL results into text format
    results_text = "\n".join([
        f"Title: {r['title']}, Description: {r['description']}, Publisher: {r['publisher']}, URL: {r['url']}"
        for r in sparql_results
    ])

    prompt = f"""
    ### Task:
    You are an intelligent assistant refining dataset search results.
    Below is a list of datasets retrieved from a **Knowledge Graph via SPARQL**.
    Your task is to **filter, rank, and return only the most relevant datasets** based on the user's query.

    ### User Query:
    "{user_query}"

    ### SPARQL Retrieved Datasets:
    {results_text}

    ### Instructions:
    - **Select only the most relevant datasets**.
    - **Rank them by relevance** (most relevant first).
    - **Discard datasets with "Low" relevance scores**.
    - **Ensure the output is structured in a JSON list**.

    ### Output Format:
    Return the datasets in JSON format:
    [
        {{
            "Title": "Dataset Title",
            "Description": "Short description",
            "URL": "Dataset URL",
            "Relevance Score": "High" or "Medium"
        }}
    ]
    """


    try:
        payload = {
            "model": LLM_MODEL_NAME,  # Use the same local LLM
            "prompt": prompt,
            "temperature": LLM_TEMPERATURE,
            "max_tokens": LLM_MAX_TOKENS,
        }
        headers = {"Content-Type": "application/json"}

        response = requests.post(LLM_API_URL, headers=headers, json=payload)
        response.raise_for_status()

        refined_results = response.json().get("response", "")

        return refined_results
    except requests.exceptions.RequestException as e:
        logger.error(f"Error communicating with LLM API: {e}")
        return "Error in LLM processing"

# Modify `main()` to refine results
def main():
    logger.info("Welcome to the Dataset Retrieval Assistant!")
    user_query = input("Enter your query (e.g., 'Show me datasets about air pollution in PDF'): ").strip()

    try:
        sparql_query = generate_sparql_query_with_llm(user_query)
        if sparql_query:
            logger.info(f"Generated SPARQL Query:\n{sparql_query}")
            results = query_knowledge_graph(sparql_query)

            if results:
                print("\nSPARQL Results Retrieved. Refining with LLM...\n")
                
                # **Pass results through LLM for final refinement**
                refined_results = refine_results_with_llm(user_query, results)
                
                print("\nFinal Refined Datasets:\n")
                print(refined_results)  # Display LLM-filtered results
                
            else:
                print("\nNo relevant datasets found.")
        else:
            print("Failed to generate a SPARQL query. Please try again.")
    except Exception as e:
        logger.error(f"An error occurred during query processing: {e}")
        print("An unexpected error occurred. Please try again.")

if __name__ == "__main__":
    main()
