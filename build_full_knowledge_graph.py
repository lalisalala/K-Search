import pandas as pd
from rdflib import Graph, URIRef, Literal, Namespace
from rdflib.namespace import RDF, RDFS
import re
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def sanitize_uri_value(value: str) -> str:
    """Sanitize the input string to ensure it's a valid URI component."""
    sanitized_value = re.sub(r'[^\w]', '_', value)
    return sanitized_value

def preprocess_metadata(value: str) -> str:
    """Preprocess metadata to clean unnecessary spaces, newlines, and format issues."""
    if not isinstance(value, str):
        return value
    # Remove excessive newlines, tabs, and carriage returns
    cleaned_value = re.sub(r'[\r\n\t]+', ' ', value)
    # Collapse multiple spaces into a single space
    cleaned_value = re.sub(r'\s+', ' ', cleaned_value)
    # Remove known formatting artifacts
    artifacts = [
        "Normal 0", "false false false", "MicrosoftInternetExplorer4", 
        "/\\* Style Definitions \\*/", "table.MsoNormalTable"
    ]
    for artifact in artifacts:
        cleaned_value = cleaned_value.replace(artifact, '')
    return cleaned_value.strip()

def build_knowledge_graph(input_csv='datasets.csv', output_rdf_file='full_metadata_ontology.ttl'):
    """
    Build a knowledge graph from the entire metadata dataset.
    Args:
        input_csv (str): Path to the metadata CSV file.
        output_rdf_file (str): Path to save the RDF knowledge graph.
    """
    g = Graph()

    # Define namespaces
    EX = Namespace("http://example.org/ontology/")
    SCHEMA = Namespace("https://schema.org/")
    SKOS = Namespace("http://www.w3.org/2004/02/skos/core#")

    g.bind("ex", EX)
    g.bind("schema", SCHEMA)
    g.bind("skos", SKOS)

    # Load metadata CSV
    try:
        df = pd.read_csv(input_csv)
    except FileNotFoundError:
        logger.error(f"Input CSV file '{input_csv}' not found.")
        return

    logger.info(f"Loaded {len(df)} datasets from '{input_csv}'.")

    # Build the graph
    for idx, row in df.iterrows():
        dataset_uri = URIRef(EX[f"Dataset_{idx + 1}"])
        g.add((dataset_uri, RDF.type, EX.Dataset))

        # Add cleaned metadata to the graph
        if pd.notna(row.get('title')):
            cleaned_title = preprocess_metadata(row['title'])
            g.add((dataset_uri, RDFS.label, Literal(cleaned_title)))

        if pd.notna(row.get('summary')):
            cleaned_summary = preprocess_metadata(row['summary'])
            g.add((dataset_uri, EX.summary, Literal(cleaned_summary)))

        if pd.notna(row.get('publisher')):
            cleaned_publisher = preprocess_metadata(row['publisher'])
            g.add((dataset_uri, EX.publisher, Literal(cleaned_publisher)))

        if pd.notna(row.get('topic')):
            topics = [preprocess_metadata(t.strip()) for t in row['topic'].split(',')]
            for topic in topics:
                topic_uri = URIRef(EX[sanitize_uri_value(topic)])
                g.add((topic_uri, RDF.type, SKOS.Concept))
                g.add((topic_uri, SKOS.prefLabel, Literal(topic)))
                g.add((dataset_uri, EX.hasTopic, topic_uri))

        if pd.notna(row.get('links')):
            cleaned_links = preprocess_metadata(row['links'])
            g.add((dataset_uri, SCHEMA.url, URIRef(cleaned_links)))

        if pd.notna(row.get('format')):
            cleaned_format = preprocess_metadata(row['format'])
            g.add((dataset_uri, EX.fileFormat, Literal(cleaned_format)))

    # Serialize the RDF graph
    g.serialize(output_rdf_file, format="turtle")
    logger.info(f"Knowledge graph saved to '{output_rdf_file}'.")

if __name__ == "__main__":
    build_knowledge_graph()
