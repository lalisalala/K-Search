import os
import requests
import pandas as pd
import logging
import yaml
import chardet
import json
import re

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    force=True  # Ensures all logs are flushed and reconfigured properly
)

logger = logging.getLogger(__name__)  # Use this logger throughout the file

# Load Configuration from config.yaml
CONFIG_FILE = "llm_config.yaml"
try:
    with open(CONFIG_FILE, "r") as file:
        config = yaml.safe_load(file)
except FileNotFoundError:
    raise RuntimeError(f"Configuration file '{CONFIG_FILE}' not found.")
except yaml.YAMLError as e:
    raise RuntimeError(f"Error parsing '{CONFIG_FILE}': {e}")

# LLM Configuration
LLM_API_URL = config["llm"]["api_url"]
LLM_MODEL_NAME = config["llm"]["model_name"]
LLM_TEMPERATURE = config["llm"]["temperature"]
LLM_MAX_TOKENS = config["llm"]["max_tokens"]

# Directory to save downloaded datasets
DOWNLOAD_DIR = "datasets"
os.makedirs(DOWNLOAD_DIR, exist_ok=True)


def download_dataset(url):
    """
    Download a dataset from the given URL.
    """
    file_name = os.path.join(DOWNLOAD_DIR, url.split("/")[-1])
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()
        with open(file_name, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        logger.info(f"Dataset downloaded: {file_name}")
        return file_name
    except requests.RequestException as e:
        logger.error(f"Failed to download dataset from {url}: {e}")
        return None


def detect_file_encoding(file_path):
    """
    Detect file encoding and print encoding details.
    """
    with open(file_path, "rb") as f:
        raw_data = f.read()
        result = chardet.detect(raw_data)
        encoding = result["encoding"]
        logger.info(f"Detected file encoding: {encoding}")
    return encoding


def preprocess_csv(file_path):
    """
    Preprocess a CSV file to clean it for analysis.
    """
    cleaned_file_path = file_path.replace(".csv", "_cleaned.csv")
    try:
        # Detect file encoding
        encoding = detect_file_encoding(file_path)

        # Read the CSV file
        df = pd.read_csv(
            file_path,
            engine="python",
            encoding=encoding,
            skipinitialspace=True,
            na_values=[".", "N/A", "", " "],
            on_bad_lines="warn",
        )

        # Normalize column names
        df.columns = (
            df.columns.str.replace(r"[^\w\s]", "_", regex=True)
            .str.replace(r"\s+", "_", regex=True)
            .str.strip()
        )

        # Drop completely empty rows and columns
        df.dropna(how="all", inplace=True)
        df.dropna(axis=1, how="all", inplace=True)

        # Save the cleaned file
        df.to_csv(cleaned_file_path, index=False)
        logger.info(f"Preprocessed CSV saved to {cleaned_file_path}")
        return cleaned_file_path, df
    except Exception as e:
        logger.error(f"Error during preprocessing: {e}")
        return None, None


def generate_llm_prompt_from_dataset(df, user_question):
    """
    Generate an informative and contextualized prompt for the LLM.
    """
    column_names = ", ".join(df.columns)
    missing_values = df.isnull().sum().to_dict()
    sample_data = df.head(5).to_string(index=False)

    prompt = (
        f"You are an assistant for open data portals helping users find and understand datasets of this portal. "
        f"The dataset has {df.shape[0]} rows and {df.shape[1]} columns. "
        f"Columns: {column_names}.\n"
        f"Missing values per column: {missing_values}.\n"
        f"Here is a sample of the data:\n{sample_data}\n\n"
        f"User Question: {user_question}\n"
        f"Please analyze the dataset based on the user question and provide a detailed response."
    )
    return prompt


import re  # For regular expressions

def clean_and_format_response(response):
    """
    Cleans and formats the LLM response into a coherent and human-readable response.
    """
    try:
        # Step 1: Normalize excessive spaces
        response = re.sub(r'\s+', ' ', response).strip()  # Replace multiple spaces with a single space

        # Step 2: Reformat numbered lists
        response = re.sub(r'(\d+)\.\s*', r'\n\1. ', response)  # Ensure numbered lists have line breaks

        # Step 3: Reformat bullet points
        response = re.sub(r'-\s+', r'\n- ', response)  # Ensure bullet points have line breaks

        # Step 4: Add line breaks after sentences
        response = re.sub(r'\.\s+', '.\n', response)  # Add line breaks after periods for readability

        # Step 5: Remove unnecessary new lines
        response = re.sub(r'\n{2,}', '\n', response)  # Replace multiple new lines with a single new line

        # Step 6: Final cleanup of trailing/leading spaces around new lines
        paragraphs = response.split('\n')
        formatted_response = "\n".join([para.strip() for para in paragraphs if para.strip()])

        return formatted_response.strip()
    except Exception as e:
        return f"Error formatting response: {e}"



def ask_llm(prompt):
    """
    Send a prompt to the LLM and return the response.
    Handles cases where the LLM response is streamed word-by-word or fragment-by-fragment.
    """
    try:
        payload = {
            "model": LLM_MODEL_NAME,
            "prompt": prompt,
            "temperature": LLM_TEMPERATURE,
            "max_tokens": LLM_MAX_TOKENS,
        }
        headers = {"Content-Type": "application/json"}

        # Request the response from the LLM API
        response = requests.post(LLM_API_URL, headers=headers, json=payload, stream=True)
        response.raise_for_status()

        # Collect text fragments from the streamed response
        fragments = []  # Store all fragments
        for line in response.iter_lines():
            if line:  # Skip empty lines
                try:
                    # Parse each JSON line
                    chunk = json.loads(line)
                    fragment = chunk.get("response", "")  # Get the 'response' text fragment
                    if fragment:
                        fragments.append(fragment)  # Add fragment to the list
                    
                    # Log each raw JSON line for debugging
                    logger.debug(f"Raw JSON Line: {line.decode('utf-8')}")

                except json.JSONDecodeError:
                    logger.warning(f"Invalid JSON chunk: {line.decode('utf-8')}")

        # Combine fragments into a full response string
        llm_response = "".join(fragments).replace("  ", " ").strip()  # Join fragments and normalize spaces

        # Log the reconstructed raw response for debugging
        logger.debug(f"Reconstructed LLM Response: {llm_response}")

        # Clean and format the response before returning
        return clean_and_format_response(llm_response)

    except Exception as e:
        logger.error(f"Error communicating with LLM: {e}")
        return "An error occurred while querying the LLM."




def analyze_dataset(file_path, user_question):
    """
    Analyze a dataset and provide insights using the LLM.
    """
    cleaned_file_path, df = preprocess_csv(file_path)
    if not cleaned_file_path or df is None:
        return "Failed to preprocess the CSV file."

    # Generate prompt for LLM
    prompt = generate_llm_prompt_from_dataset(df, user_question)

    # Get response from LLM and clean it
    response = ask_llm(prompt)
    return response



def explore_dataset_via_ui(url, user_question):
    """
    Download, preprocess, and analyze a dataset via the UI.
    Incrementally yield the response for real-time updates.
    """
    file_path = download_dataset(url)
    if not file_path:
        yield "Failed to download the dataset."
        return

    cleaned_file_path, df = preprocess_csv(file_path)
    if not cleaned_file_path or df is None:
        yield "Failed to preprocess the CSV file."
        return

    # Generate prompt for LLM
    prompt = generate_llm_prompt_from_dataset(df, user_question)

    # Stream the LLM response incrementally
    try:
        payload = {
            "model": LLM_MODEL_NAME,
            "prompt": prompt,
            "temperature": LLM_TEMPERATURE,
            "max_tokens": LLM_MAX_TOKENS,
        }
        headers = {"Content-Type": "application/json"}

        # Stream the response from the LLM API
        response = requests.post(LLM_API_URL, headers=headers, json=payload, stream=True)
        response.raise_for_status()

        for line in response.iter_lines():
            if line:
                try:
                    chunk = json.loads(line)
                    fragment = chunk.get("response", "")
                    if fragment:
                        yield fragment  # Yield fragments (words/phrases) incrementally
                except json.JSONDecodeError:
                    yield "Error processing LLM response chunk."

    except requests.RequestException as e:
        yield f"Error communicating with LLM: {e}"
