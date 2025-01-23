import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Step 1: Load the similarity matrix and dataset IDs
def load_similarity_matrix(file_path):
    """Load similarity matrix and dataset IDs from .npy file."""
    data = np.load(file_path, allow_pickle=True).item()
    similarity_matrix = data["matrix"]
    dataset_ids = data["ids"]
    return similarity_matrix, dataset_ids

# Step 2: Visualize the similarity matrix as a heatmap
def plot_similarity_heatmap(similarity_matrix, output_file="similarity_heatmap.png"):
    """Generate and save a heatmap of the similarity matrix."""
    plt.figure(figsize=(10, 8))
    sns.heatmap(similarity_matrix, xticklabels=False, yticklabels=False, cmap="coolwarm", cbar=True)
    plt.title("Similarity Matrix Heatmap")
    plt.xlabel("Dataset Index")
    plt.ylabel("Dataset Index")
    plt.tight_layout()
    plt.savefig(output_file)
    plt.show()
    print(f"Heatmap saved to {output_file}")

# Step 3: Extract top similar dataset pairs
def get_top_similar_pairs(similarity_matrix, dataset_ids, top_n=10):
    """Extract and display the top N similar dataset pairs."""
    similarities = []
    for i in range(len(dataset_ids)):
        for j in range(i + 1, len(dataset_ids)):  # Only consider upper triangle
            similarities.append((dataset_ids[i], dataset_ids[j], similarity_matrix[i, j]))
    
    # Sort by similarity score (descending)
    similarities = sorted(similarities, key=lambda x: x[2], reverse=True)
    
    # Convert to DataFrame
    similarities_df = pd.DataFrame(similarities, columns=["Dataset 1", "Dataset 2", "Similarity Score"])
    
    # Save to CSV
    similarities_df.to_csv("top_similar_pairs.csv", index=False)
    print("Top similar pairs saved to top_similar_pairs.csv")
    
    return similarities_df.head(top_n)

# Main Function
if __name__ == "__main__":
    # File paths
    similarity_file = "similarity_matrix.npy"
    heatmap_file = "similarity_heatmap.png"
    
    # Load similarity matrix
    print("Loading similarity matrix...")
    similarity_matrix, dataset_ids = load_similarity_matrix(similarity_file)
    print(f"Similarity Matrix Shape: {similarity_matrix.shape}")
    print(f"Total Datasets: {len(dataset_ids)}")
    
    # Plot heatmap
    print("Generating heatmap...")
    plot_similarity_heatmap(similarity_matrix, output_file=heatmap_file)
    
    # Get top similar pairs
    print("Extracting top similar pairs...")
    top_pairs = get_top_similar_pairs(similarity_matrix, dataset_ids, top_n=10)
    print("Top Similar Dataset Pairs:")
    print(top_pairs)
