import json
import urllib.parse
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
from bert_score import score
import torch
import re

# 📂 Load datasets
def load_json(filename):
    try:
        with open(filename, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        print(f"Error loading {filename}: {e}")
        return []

ground_truth_data = load_json("ground_truth.json")
keywordsearch_data = load_json("keywordsearch.json")
sparql_data = load_json("SPARQL_results.json")
faiss_data = load_json("FAISS_results.json")

# 🔍 Clean HTML tags from descriptions
def clean_html(text):
    clean = re.compile('<.*?>')
    return re.sub(clean, '', text)

# 🔍 Compute BERT Score
def compute_bertscore(relevant_texts, retrieved_texts):
    if not retrieved_texts or not relevant_texts:
        print("⚠️ Warning: No valid text to compare.")
        return 0, 0, 0

    relevant_texts = [clean_html(text) for text in relevant_texts]
    retrieved_texts = [clean_html(text) for text in retrieved_texts]
    
    min_len = min(len(relevant_texts), len(retrieved_texts))
    relevant_texts, retrieved_texts = relevant_texts[:min_len], retrieved_texts[:min_len]
    
    try:
        device = "cuda" if torch.cuda.is_available() else "cpu"
        print(f"Using device: {device}")  # Debugging line
        P, R, F1 = score(retrieved_texts, relevant_texts, model_type="bert-base-uncased", lang="en", device=device)
        print("Raw Recall Scores:", R)
        print("Mean Recall:", R.mean().item())

        return P.mean().item(), R.mean().item(), F1.mean().item()
    except Exception as e:
        print(f"Error computing BERTScore: {e}")
        return 0, 0, 0

# 🔍 Evaluate models against ground truth
def evaluate_model_bert(model_data, model_name):
    query_metrics = []

    for gt_entry in ground_truth_data:
        query = gt_entry["keyword_search"]

        # Extract relevant descriptions from ground truth
        relevant_texts = [ds.get("description", "No description available.") for ds in gt_entry.get("retrieved_datasets", [])]

        # Extract retrieved descriptions from model results
        model_entry = next((entry for entry in model_data if entry["keyword_search"] == query), None)
        retrieved_texts = [ds.get("description", "No description available.") for ds in model_entry.get("retrieved_datasets", [])] if model_entry else []

        # Compute BERT Score
        print(f"Processing query: {query}")  # Debugging line
        bert_precision, bert_recall, bert_f1 = compute_bertscore(relevant_texts, retrieved_texts)

        query_metrics.append({"query": query, "bert_precision": bert_precision, "bert_recall": bert_recall, "bert_f1_score": bert_f1})

    # Convert results into DataFrame & save CSV
    df_results = pd.DataFrame(query_metrics)
    df_results.to_csv(f"{model_name}_bert_evaluation_results.csv", index=False)

    return df_results

# 🔬 Run BERT evaluations
keyword_bert_results = evaluate_model_bert(keywordsearch_data, "keywordsearch")
sparql_bert_results = evaluate_model_bert(sparql_data, "SPARQL")
faiss_bert_results = evaluate_model_bert(faiss_data, "FAISS")

# 📊 **Multi-Plot Comparison**
queries = keyword_bert_results["query"]
x = np.arange(len(queries))
width = 0.3

fig, axes = plt.subplots(1, 3, figsize=(18, 6), sharey=True)
metrics = ["bert_precision", "bert_recall", "bert_f1_score"]
titles = ["BERT Precision", "BERT Recall", "BERT F1-Score"]
colors = {"Keyword": "#1f77b4", "SPARQL": "#ff7f0e", "FAISS": "#2ca02c"}

for i, metric in enumerate(metrics):
    ax = axes[i]
    ax.bar(x - width, keyword_bert_results[metric], width=width, color=colors["Keyword"])
    ax.bar(x, sparql_bert_results[metric], width=width, color=colors["SPARQL"])
    ax.bar(x + width, faiss_bert_results[metric], width=width, color=colors["FAISS"])
    ax.set_title(titles[i], fontsize=14, fontweight="bold")
    ax.set_xlabel("Queries", fontsize=12)
    ax.set_xticks(x)
    ax.set_xticklabels(queries, rotation=45, ha="right", fontsize=10)
    ax.grid(axis="y", linestyle="--", alpha=0.6)

axes[0].set_ylabel("Score", fontsize=12)
fig.legend(["Keyword", "SPARQL", "FAISS"], loc="upper right")
plt.tight_layout()
plt.savefig("bert_comparison_multi_plot.png", dpi=300, bbox_inches="tight")
plt.show()


# 🏆 **Overall BERTScore Model Ranking**
def rank_bert_models():
    avg_scores = {
        "Keyword": [keyword_bert_results[metric].mean() for metric in metrics],
        "SPARQL": [sparql_bert_results[metric].mean() for metric in metrics],
        "FAISS": [faiss_bert_results[metric].mean() for metric in metrics],
    }
    df_ranking = pd.DataFrame(avg_scores, index=metrics)
    df_ranking.plot(kind="bar", figsize=(8, 5), color=[colors[m] for m in df_ranking.columns])
    plt.ylabel("Average Score")
    plt.title("Model Performance Across BERT Metrics")
    plt.xticks(rotation=0)
    plt.legend(title="Models", loc="upper left", bbox_to_anchor=(1,1))
    plt.savefig("bert_overall_model_ranking.png", dpi=300, bbox_inches="tight")
    plt.show()

rank_bert_models()
