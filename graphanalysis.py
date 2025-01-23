from rdflib import Graph, Namespace, URIRef
import logging
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Define namespaces
EX = Namespace("http://example.org/ontology/")
RDFS = Namespace("http://www.w3.org/2000/01/rdf-schema#")
SCHEMA = Namespace("https://schema.org/")

# Load the RDF Knowledge Graph
def load_graph(rdf_file):
    """
    Load the RDF knowledge graph from the given Turtle file.
    """
    g = Graph()
    try:
        g.parse(rdf_file, format="turtle")
        logger.info(f"Graph loaded successfully with {len(g)} triples.")
        return g
    except Exception as e:
        logger.error(f"Error loading RDF file '{rdf_file}': {e}")
        return None

def analyze_missing_metadata(graph):
    """
    Identify datasets with missing metadata fields and calculate percentages.
    """
    expected_fields = {
        "label": RDFS.label,
        "summary": EX.summary,
        "fileFormat": EX.fileFormat,
        "url": SCHEMA.url,
        "publisher": EX.publisher,
        "group": EX.hasGroup,
        "tag": EX.hasTag
    }
    
    results = []
    missing_count = {field: 0 for field in expected_fields.keys()}
    total_datasets = 0

    for dataset in graph.subjects(predicate=None, object=EX.Dataset):
        total_datasets += 1
        dataset_uri = str(dataset)
        missing_fields = []

        # Check for each expected field
        for field_name, field_uri in expected_fields.items():
            if not (dataset, field_uri, None) in graph:
                missing_fields.append(field_name)
                missing_count[field_name] += 1
            else:
                # Handle special cases like "Unknown_Group" or "Unknown_Tag"
                if field_name in ["group", "tag"]:
                    for value in graph.objects(dataset, field_uri):
                        if value == EX.Unknown_Group or value == EX.Unknown_Tag:
                            missing_fields.append(field_name)
                            missing_count[field_name] += 1
                            break

        results.append({
            "Dataset": dataset_uri,
            "Missing Metadata": ", ".join(missing_fields) if missing_fields else "None"
        })
    
    if total_datasets == 0:
        logger.warning("No datasets found in the graph.")
        return results, missing_count, {}

    missing_percentage = {
        field: (count / total_datasets) * 100 for field, count in missing_count.items()
    }

    return results, missing_count, missing_percentage



# Generate a Summary Report
def generate_report(results, output_file="metadata_analysis_report.csv"):
    """
    Generate a CSV report of the metadata analysis.
    """
    expanded_results = []
    for result in results:
        dataset = result["Dataset"]
        for field in result["Missing Metadata"]:
            expanded_results.append({"Dataset": dataset, "Missing Metadata": field})
    
    df = pd.DataFrame(expanded_results)
    logger.info(f"Saving report to {output_file}")
    df.to_csv(output_file, index=False)
    logger.info(f"Report saved successfully to {output_file}.")
    return df

def plot_dataset_missing_metadata(results, max_datasets=50):
    """
    Plot a bar chart of missing metadata for each dataset.
    """
    sns.set_theme(style="whitegrid")
    
    # Convert results to a DataFrame
    df = pd.DataFrame(results)
    
    # Limit the number of datasets for visualization
    if len(df) > max_datasets:
        logger.warning(f"Too many datasets ({len(df)}). Displaying only the first {max_datasets}.")
        df = df.head(max_datasets)
    
    # Create a new DataFrame for plotting
    missing_data = pd.DataFrame(df["Missing Metadata"].str.split(", ").tolist(), index=df["Dataset"]).stack()
    missing_data = missing_data.reset_index()[[0, 'Dataset']]  # Reset index and keep columns
    missing_data.columns = ['Missing Field', 'Dataset']
    
    # Plot the data
    plt.figure(figsize=(14, 8))  # Adjust the size as needed
    ax = sns.countplot(data=missing_data, y="Dataset", hue="Missing Field", palette="coolwarm")
    
    # Customize the plot
    ax.set_title("Missing Metadata by Dataset", fontsize=20, weight="bold")
    ax.set_xlabel("Count of Missing Fields", fontsize=14, weight="bold")
    ax.set_ylabel("Dataset", fontsize=14, weight="bold")
    plt.xticks(fontsize=12)
    plt.yticks(fontsize=12)
    plt.legend(title="Missing Field", fontsize=12, title_fontsize=14)
    
    # Adjust layout
    plt.tight_layout()
    plt.savefig("missing_metadata_by_dataset.png", dpi=300, bbox_inches="tight")
    plt.show()
    logger.info("Bar chart of missing metadata saved as 'missing_metadata_by_dataset.png'.")


def main():
    rdf_file = "full_metadata_ontology.ttl"  # Update with your RDF file name
    graph = load_graph(rdf_file)
    
    if graph:
        # Analyze missing metadata
        try:
            results, missing_count, missing_percentage = analyze_missing_metadata(graph)
        except ValueError as e:
            logger.error(f"Error analyzing metadata: {e}")
            return

        # Generate the CSV report
        report = generate_report(results)
        print(report)

        # Display the missing percentages
        if missing_percentage:
            print("\nPercentage of Missing Metadata:")
            for field, percentage in missing_percentage.items():
                print(f"{field}: {percentage:.2f}%")
        
        # Plot the missing metadata for each dataset
        plot_dataset_missing_metadata(results)



if __name__ == "__main__":
    main()
