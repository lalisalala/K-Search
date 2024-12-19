from rdflib import Graph, Namespace, URIRef, Literal
import logging
from llm_chatbot import LLMChatbot
from graphanalysis import analyze_missing_metadata, load_graph  # Import analysis functions

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Define namespaces
EX = Namespace("http://example.org/ontology/")
RDFS = Namespace("http://www.w3.org/2000/01/rdf-schema#")
SCHEMA = Namespace("https://schema.org/")

# Initialize the LLMChatbot
chatbot = LLMChatbot(model_name="mistral", temperature=0, max_tokens=1000, api_url="http://localhost:11434/api/generate")

PREDEFINED_GROUPS = [
    "Demographics", "Environment", "Employment and Skills", "Planning", "Transparency",
    "Business and Economy", "Housing", "Health", "Education", "Transport",
    "Crime and Community Safety", "Young People", "Income, Poverty, and Welfare",
    "Art and Culture", "COVID-19 Data and Analysis", "Championing London",
    "Sport", "London 2012"
]

def generate_metadata_with_llm(dataset_uri, existing_metadata, missing_fields):
    """
    Use the LLM to generate missing metadata for a dataset, classifying groups into predefined categories.
    """
    group_list_str = ", ".join(PREDEFINED_GROUPS)

    prompt = (
        f"You are an assistant tasked with generating fitting metadata for datasets in an RDF knowledge graph.\n"
        f"Here is the existing metadata:\n"
        f"- Label: {existing_metadata.get('label', 'N/A')}\n"
        f"- Summary: {existing_metadata.get('summary', 'N/A')}\n"
        f"- File Format: {existing_metadata.get('fileFormat', 'N/A')}\n"
        f"- Publisher: {existing_metadata.get('publisher', 'N/A')}\n\n"
        f"The following metadata fields are missing: {', '.join(missing_fields)}.\n"
        f"When generating groups, classify them into one or more of these predefined categories: {group_list_str}.\n\n"
        f"Important Instructions:\n"
        f"- Only output the group names. Do not include explanations, descriptions, or additional text.\n"
        f"- Use the exact names from the predefined categories provided above.\n\n"
        f"Output the generated metadata as:\n"
        f"- group: <Generated Group(s)>\n"
        f"Example Output:\n"
        f"- group: Planning, Transparency, Business and Economy"
    )


    logger.info(f"Sending prompt to LLM for dataset {dataset_uri}.")

    try:
        llm_response = chatbot.generate_response(context="", query=prompt)

        # Extract generated metadata from the response
        metadata = {}
        for line in llm_response.splitlines():
            if line.startswith("- group:"):
                groups = [g.strip() for g in line.replace("- group:", "").split(",")]
                # Validate groups against predefined list
                metadata["group"] = [group for group in groups if group in PREDEFINED_GROUPS][:5]  # Limit to max 5 groups

        if metadata:
            logger.info(f"Metadata generation successful for dataset {dataset_uri}: {metadata}")
        else:
            logger.warning(f"No metadata generated for dataset {dataset_uri}. Response:\n{llm_response}")

        return metadata
    except Exception as e:
        logger.error(f"Error generating metadata for dataset {dataset_uri}: {e}")
        return None

def update_graph_with_metadata(graph, updates):
    """
    Update the RDF graph with generated metadata, ensuring groups are linked to existing categories.
    Add new groups even if some already exist, avoiding duplicates.
    """
    for dataset_uri, metadata in updates.items():
        dataset = URIRef(dataset_uri)

        # Collect existing groups
        existing_groups = {str(g) for g in graph.objects(dataset, EX.hasGroup)}

        if "group" in metadata:
            # Prepare new groups, avoiding duplicates
            for group in metadata["group"][:5]:  # Limit to 5 new groups
                group_uri = URIRef(EX[group.replace(" ", "_")])
                if str(group_uri) not in existing_groups:
                    graph.add((dataset, EX.hasGroup, group_uri))
                    logger.info(f"Added new group '{group}' to dataset {dataset_uri}.")
                else:
                    logger.info(f"Group '{group}' already exists for dataset {dataset_uri}.")
    return graph

def main():
    rdf_file = "full_metadata_ontology2.ttl"
    output_file = "updated_metadata_ontology3.ttl"

    # Load the graph
    graph = load_graph(rdf_file)
    if not graph:
        return

    # Initialize updates dictionary
    updates = {}

    # Process all datasets in the graph
    for dataset in graph.subjects(predicate=None, object=EX.Dataset):
        dataset_uri = str(dataset)

        # Extract existing metadata for the dataset
        existing_metadata = {
            "label": str(graph.value(dataset, RDFS.label, default="")),
            "summary": str(graph.value(dataset, EX.summary, default="")),
            "fileFormat": str(graph.value(dataset, EX.fileFormat, default="")),
            "publisher": str(graph.value(dataset, EX.publisher, default="")),
            "existingGroups": [str(g) for g in graph.objects(dataset, EX.hasGroup)]  # Existing groups
        }

        # Generate additional groups using the LLM
        generated_metadata = generate_metadata_with_llm(dataset_uri, existing_metadata, ["group"])

        # Collect updates
        if generated_metadata:
            updates[dataset_uri] = generated_metadata
            logger.info(f"Successfully generated additional groups for dataset {dataset_uri}.")
        else:
            logger.warning(f"Skipping dataset {dataset_uri} due to failed group generation.")

    # Update the graph with additional group metadata
    graph = update_graph_with_metadata(graph, updates)

    # Save the updated graph
    graph.serialize(output_file, format="turtle")
    logger.info(f"Updated graph saved to {output_file}")



if __name__ == "__main__":
    main()
