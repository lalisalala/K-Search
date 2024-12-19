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

    logger.debug("Starting metadata analysis...")

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
    
    missing_percentage = {
        field: (count / total_datasets) * 100 for field, count in missing_count.items()
    }

    logger.debug(f"Metadata analysis complete. Total datasets: {total_datasets}")
    logger.debug(f"Missing metadata counts: {missing_count}")
    logger.debug(f"Missing metadata percentages: {missing_percentage}")

    return results, missing_count, missing_percentage


# Generate a Summary Report
def generate_report(results, output_file="metadata_analysis_report.csv"):
    """
    Generate a CSV report of the metadata analysis.
    """
    df = pd.DataFrame(results)
    logger.info(f"Saving report to {output_file}")
    df.to_csv(output_file, index=False)
    logger.info(f"Report saved successfully to {output_file}.")
    return df

def plot_missing_metadata(missing_count, missing_percentage):
    """
    Plot a visually enhanced bar chart of the missing metadata counts with percentages using Seaborn.
    """
    sns.set_theme(style="whitegrid")

    # Create a DataFrame for easier plotting
    df = pd.DataFrame({
        "Metadata Field": list(missing_count.keys()),
        "Missing Count": list(missing_count.values()),
        "Missing Percentage": [f"{p:.2f}%" for p in missing_percentage.values()]
    })

    # Create the bar plot
    plt.figure(figsize=(14, 8))  # Wider plot for better readability
    ax = sns.barplot(
        x="Metadata Field",
        y="Missing Count",
        data=df,
        palette="coolwarm",
        dodge=False
    )

    # Customize the plot
    ax.set_title(
        "Analysis of Missing Metadata",
        fontsize=20,
        weight="bold",
        color="#333333"
    )
    ax.set_xlabel(
        "Metadata Field",
        fontsize=14,
        weight="bold",
        labelpad=10
    )
    ax.set_ylabel(
        "Number of Missing Entries",
        fontsize=14,
        weight="bold",
        labelpad=10
    )

    # Add counts and percentages above bars
    for i, bar in enumerate(ax.patches):
        height = bar.get_height()
        percentage = missing_percentage[list(missing_count.keys())[i]]
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            height + 0.5,
            f"{height} ({percentage:.2f}%)",
            ha="center",
            fontsize=12,
            weight="bold",
            color="#333333"
        )

    # Enhance x-ticks and gridlines
    plt.xticks(rotation=45, fontsize=12, weight="bold", color="#333333")
    plt.yticks(fontsize=12, weight="bold", color="#333333")
    plt.grid(axis="y", linestyle="--", alpha=0.7)

    # Add background styling
    ax.set_facecolor("#f9f9f9")
    plt.gcf().set_facecolor("#f9f9f9")

    # Adjust layout for better spacing
    plt.tight_layout()

    # Save and show the plot
    plt.savefig("enhanced_missing_metadata_plot.png", dpi=300, bbox_inches="tight")
    plt.show()
    logger.info("Enhanced missing metadata plot saved as 'enhanced_missing_metadata_plot2.png'.")

def main():
    rdf_file = "full_metadata_ontology.ttl"  # Update with your RDF file name
    graph = load_graph(rdf_file)
    
    if graph:
        # Unpack three values: results, missing_count, and missing_percentage
        results, missing_count, missing_percentage = analyze_missing_metadata(graph)
        
        # Generate the CSV report
        report = generate_report(results)
        print(report)

        # Display the missing percentages
        print("\nPercentage of Missing Metadata:")
        for field, percentage in missing_percentage.items():
            print(f"{field}: {percentage:.2f}%")
        
        # Plot the missing metadata
        plot_missing_metadata(missing_count, missing_percentage)

if __name__ == "__main__":
    main()
