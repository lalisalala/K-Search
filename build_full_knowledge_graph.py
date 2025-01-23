from rdflib import Graph, URIRef, Literal, Namespace
from rdflib.namespace import RDF, DCTERMS, DCAT, FOAF, SKOS
import json
import logging
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity
import numpy as np

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Define namespaces
PROJECT_NS = "http://yourprojectname.org/"
EX = Namespace(f"{PROJECT_NS}ontology/")
RESOURCE_NS = f"{PROJECT_NS}resource/"
DATASET_NS = f"{PROJECT_NS}dataset/"
PUBLISHER_NS = f"{PROJECT_NS}publisher/"
GROUP_NS = f"{PROJECT_NS}group/"
TAG_NS = f"{PROJECT_NS}tag/"

def clean_text(text):
    """Normalize text by removing extra spaces and handling None values."""
    if not text:
        return "Unknown"
    return " ".join(text.strip().split())

def extract_year(date_str):
    """Extract the year from an ISO date string."""
    try:
        from datetime import datetime
        return datetime.fromisoformat(date_str.replace("Z", "")).year
    except ValueError:
        return None

def build_knowledge_graph(input_file="datasets.json", output_rdf_file="knowledge_graph.ttl", similarity_file="similarity_matrix.npy", similarity_threshold=0.8):
    """Build a Knowledge Graph with standardized ontology."""
    g = Graph()
    g.bind("dcat", DCAT)
    g.bind("dcterms", DCTERMS)
    g.bind("foaf", FOAF)
    g.bind("skos", SKOS)
    g.bind("ex", EX)

    # Load metadata
    try:
        with open(input_file, "r") as f:
            datasets = json.load(f)
    except Exception as e:
        logger.error(f"Error loading metadata file: {e}")
        return

    # Initialize Sentence Transformer model
    model = SentenceTransformer('all-mpnet-base-v2')

    dataset_uris = []
    dataset_summaries = []
    dataset_ids = []

    # Add datasets and their metadata
    for dataset in datasets:
        dataset_uri = URIRef(f"{DATASET_NS}{dataset['id']}")
        dataset_uris.append(dataset_uri)
        dataset_ids.append(dataset["id"])

        g.add((dataset_uri, RDF.type, DCAT.Dataset))

        # Add core metadata
        title = clean_text(dataset["title"])
        summary = clean_text(dataset["summary"])
        g.add((dataset_uri, DCTERMS.title, Literal(title)))
        g.add((dataset_uri, DCTERMS.description, Literal(summary)))

        # Publisher
        publisher_name = clean_text(dataset.get("publisher", "Unknown Publisher"))
        if publisher_name != "Unknown Publisher":
            publisher_uri = URIRef(f"{PUBLISHER_NS}{publisher_name.replace(' ', '_')}")
            g.add((publisher_uri, RDF.type, FOAF.Agent))
            g.add((publisher_uri, FOAF.name, Literal(publisher_name)))
            g.add((dataset_uri, DCTERMS.publisher, publisher_uri))

        # Tags (SKOS Concepts)
        for tag in dataset.get("tags", []):
            tag_clean = clean_text(tag)
            tag_uri = URIRef(f"{TAG_NS}{tag_clean.replace(' ', '_')}")
            g.add((tag_uri, RDF.type, SKOS.Concept))
            g.add((tag_uri, SKOS.prefLabel, Literal(tag_clean)))
            g.add((dataset_uri, DCAT.keyword, tag_uri))

        # Groups (replacing ex:hasGroup with dcat:theme)
        for group in dataset.get("groups", []):
            group_clean = clean_text(group)
            group_uri = URIRef(f"{GROUP_NS}{group_clean.replace(' ', '_')}")
            g.add((group_uri, RDF.type, SKOS.Concept))
            g.add((group_uri, SKOS.prefLabel, Literal(group_clean)))
            g.add((dataset_uri, DCAT.theme, group_uri))

        # Temporal Metadata
        created_year = extract_year(dataset.get("metadata_created", ""))
        modified_year = extract_year(dataset.get("metadata_modified", ""))
        if created_year:
            g.add((dataset_uri, DCTERMS.created, Literal(created_year, datatype="http://www.w3.org/2001/XMLSchema#gYear")))
        if modified_year:
            g.add((dataset_uri, DCTERMS.modified, Literal(modified_year, datatype="http://www.w3.org/2001/XMLSchema#gYear")))

        # Resources (DCAT Distribution)
        for resource in dataset.get("resources", []):
            resource_url = resource.get("url")
            resource_format = clean_text(resource.get("format", "Unknown Format"))
            resource_uri = URIRef(f"{RESOURCE_NS}{resource['id']}")
            g.add((resource_uri, RDF.type, DCAT.Distribution))
            if resource_url:
                g.add((resource_uri, DCAT.downloadURL, URIRef(resource_url)))
            g.add((resource_uri, DCAT.mediaType, Literal(resource_format)))
            g.add((dataset_uri, DCAT.distribution, resource_uri))

        # Combine title and summary for semantic similarity
        combined_text = f"{title} {summary}"
        dataset_summaries.append(combined_text)

    # Compute semantic embeddings
    logger.info("Computing embeddings...")
    dataset_embeddings = model.encode(dataset_summaries)

    # Compute pairwise semantic similarity
    logger.info("Computing similarity matrix...")
    similarity_matrix = cosine_similarity(dataset_embeddings)

    # Add 'similarTo' relationships based on semantic similarity
    for i in range(len(dataset_uris)):
        for j in range(i + 1, len(dataset_uris)):
            if similarity_matrix[i][j] > similarity_threshold:
                g.add((dataset_uris[i], EX.similarTo, dataset_uris[j]))
                g.add((dataset_uris[j], EX.similarTo, dataset_uris[i]))

    # Serialize the Knowledge Graph
    g.serialize(output_rdf_file, format="turtle")
    logger.info(f"Knowledge Graph saved to '{output_rdf_file}'")

    # Save similarity matrix for inspection
    logger.info("Saving similarity matrix for inspection...")
    np.save(similarity_file, {"matrix": similarity_matrix, "ids": dataset_ids})
    logger.info(f"Similarity matrix saved to '{similarity_file}'")

if __name__ == "__main__":
    build_knowledge_graph()
