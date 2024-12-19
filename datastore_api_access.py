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
        return "Unknown Value"
    # Remove excessive newlines, tabs, and carriage returns
    cleaned_value = re.sub(r'[\r\n\t]+', ' ', value)
    # Collapse multiple spaces into a single space
    cleaned_value = re.sub(r'\s+', ' ', cleaned_value)
    return cleaned_value.strip()

def build_knowledge_graph(input_csv='datasets.csv', output_rdf_file='full_metadata_ontology.ttl'):
    """
    Build a knowledge graph from the metadata dataset.
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
        title = preprocess_metadata(row.get('title', "Unknown Title"))
        g.add((dataset_uri, RDFS.label, Literal(title)))

        summary = preprocess_metadata(row.get('summary', "Unknown Summary"))
        g.add((dataset_uri, EX.summary, Literal(summary)))

        publisher = preprocess_metadata(row.get('publisher', "Unknown Publisher"))
        g.add((dataset_uri, EX.publisher, Literal(publisher)))

        # Add tags (specific topics)
        if pd.notna(row.get('tags')):
            tags = [preprocess_metadata(t.strip()) for t in row['tags'].split(',')]
        else:
            tags = ["Unknown Tag"]

        for tag in tags:
            tag_uri = URIRef(EX[sanitize_uri_value(tag)])
            g.add((tag_uri, RDF.type, SKOS.Concept))
            g.add((tag_uri, SKOS.prefLabel, Literal(tag)))
            g.add((dataset_uri, EX.hasTag, tag_uri))

        # Add groups (broader categories)
        if pd.notna(row.get('groups')):
            groups = [preprocess_metadata(group.strip()) for group in row['groups'].split(',')]
        else:
            groups = ["Unknown Group"]

        for group in groups:
            group_uri = URIRef(EX[sanitize_uri_value(group)])
            g.add((group_uri, RDF.type, SKOS.Concept))
            g.add((group_uri, SKOS.prefLabel, Literal(group)))
            g.add((dataset_uri, EX.hasGroup, group_uri))

        links = preprocess_metadata(row.get('links', "Unknown Links"))
        g.add((dataset_uri, SCHEMA.url, URIRef(links)))

        file_format = preprocess_metadata(row.get('format', "Unknown Format"))
        g.add((dataset_uri, EX.fileFormat, Literal(file_format)))

    # Serialize the RDF graph
    g.serialize(output_rdf_file, format="turtle")
    logger.info(f"Knowledge graph saved to '{output_rdf_file}'.")

if __name__ == "__main__":
    build_knowledge_graph()
