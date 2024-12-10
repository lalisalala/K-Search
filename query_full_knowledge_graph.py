from rdflib import Graph
import logging
import re
from llm_chatbot import LLMChatbot

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize the LLMChatbot with Mistral
chatbot = LLMChatbot(model_name="mistral", temperature=0.7, max_tokens=500, api_url="http://localhost:11434/api/generate")

# Static SPARQL Query Template
SPARQL_TEMPLATE = """
PREFIX ex: <http://example.org/ontology/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX schema: <https://schema.org/>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>

SELECT DISTINCT ?dataset ?label ?summary ?fileFormat ?url ?publisher
WHERE {{
  ?dataset a ex:Dataset .
  OPTIONAL {{ ?dataset rdfs:label ?label . }}
  OPTIONAL {{ ?dataset ex:summary ?summary . }}
  OPTIONAL {{ ?dataset ex:fileFormat ?fileFormat . }}
  OPTIONAL {{ ?dataset schema:url ?url . }}
  OPTIONAL {{ ?dataset ex:publisher ?publisher . }}
  {filters}
}}
"""

def parse_user_query(user_query):
    """
    Parse the user query to extract facets like topic, format, and publisher.
    """
    format_pattern = re.compile(r'\b(csv|json|html|pdf|excel|zip|doc|xls|spreadsheet)\b', re.IGNORECASE)
    topic_pattern = re.compile(r'about (.+)', re.IGNORECASE)
    publisher_pattern = re.compile(r'from ([\w\s]+)', re.IGNORECASE)

    format_match = format_pattern.search(user_query)
    dataset_format = format_match.group(0).lower() if format_match else None

    topic_match = topic_pattern.search(user_query)
    topic = topic_match.group(1).strip() if topic_match else None

    publisher_match = publisher_pattern.search(user_query)
    publisher = publisher_match.group(1).strip() if publisher_match else None

    return {'format': dataset_format, 'topic': topic, 'publisher': publisher}

def build_filters(facets):
    """
    Build the SPARQL FILTER block based on the extracted facets.
    """
    filters = []
    if facets.get('topic'):
        filters.append(f'FILTER (REGEX(LCASE(STR(?label)), "{facets["topic"].lower()}", "i") || REGEX(LCASE(STR(?summary)), "{facets["topic"].lower()}", "i"))')
    if facets.get('format'):
        filters.append(f'FILTER (REGEX(LCASE(STR(?fileFormat)), "{facets["format"].lower()}", "i"))')
    if facets.get('publisher'):
        filters.append(f'FILTER (REGEX(LCASE(STR(?publisher)), "{facets["publisher"].lower()}", "i"))')
    return "\n  ".join(filters)

def generate_sparql_query_with_llm(user_query, facets):
    """
    Use the LLM to analyze the user query and generate a SPARQL query using the template.
    """
    filters = build_filters(facets)
    final_query = SPARQL_TEMPLATE.format(filters=filters)
    logger.info(f"Generated SPARQL Query:\n{final_query}")
    return final_query

def query_knowledge_graph(query_string, rdf_file='full_metadata_ontology.ttl'):
    """
    Query the RDF knowledge graph with the SPARQL query.
    """
    g = Graph()
    try:
        g.parse(rdf_file, format="turtle")
        logger.info(f"Graph contains {len(g)} triples.")
    except FileNotFoundError:
        logger.error(f"RDF file '{rdf_file}' not found.")
        return []

    try:
        logger.info(f"Executing SPARQL Query:\n{query_string}")
        results = g.query(query_string)
        return [{str(var): row[var] for var in row.labels} for row in results]
    except Exception as e:
        logger.error(f"Error executing SPARQL query: {e}")
        return []

def main():
    user_query = input("Enter your dataset query (e.g., 'I am looking for a dataset in csv format about air quality'): ").strip()
    facets = parse_user_query(user_query)
    logger.info(f"Extracted Facets: {facets}")

    sparql_query = generate_sparql_query_with_llm(user_query, facets)
    results = query_knowledge_graph(sparql_query)
    if results:
        print("\nRelevant Datasets Found:\n")
        for idx, result in enumerate(results, start=1):
            print(f"Dataset {idx}:")
            print(f"Title: {result.get('label', 'N/A')}")
            print(f"Summary: {result.get('summary', 'N/A')}")
            print(f"File Format: {result.get('fileFormat', 'N/A')}")
            print(f"Publisher: {result.get('publisher', 'N/A')}")
            print(f"Link: {result.get('url', 'N/A')}")
            print()
    else:
        print("\nNo relevant datasets found.")

if __name__ == "__main__":
    main()
