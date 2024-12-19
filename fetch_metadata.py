import requests
import os
import json

# Define the URL of the dataset and the metadata endpoint
DATASET_URL = "https://data.london.gov.uk/dataset/green-jobs-and-skills-postings"
API_ENDPOINT = "https://data.london.gov.uk/api/3/action/package_show"

# Extract the dataset identifier from the URL (last part of the path)
data_identifier = DATASET_URL.rstrip("/").split("/")[-1]

# Define the parameters for the API call
params = {
    "id": data_identifier
}

try:
    # Make a request to the metadata endpoint
    response = requests.get(API_ENDPOINT, params=params)
    response.raise_for_status()

    # Parse the JSON response
    metadata = response.json()

    # Define the output file path
    output_file = os.path.join(os.getcwd(), f"{data_identifier}_metadata.json")

    # Save the JSON metadata to a file
    with open(output_file, "w") as f:
        json.dump(metadata, f, indent=4)

    print(f"Metadata successfully retrieved and saved to: {output_file}")
except requests.exceptions.RequestException as e:
    print(f"An error occurred while fetching the metadata: {e}")
except Exception as e:
    print(f"An unexpected error occurred: {e}")
