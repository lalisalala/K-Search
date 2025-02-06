import requests
import json
import re
import urllib.parse
import time

API_KEY = "c535ba42-c70c-4cf0-826c-6a93bc6b1f2c"
BASE_URL = "https://data.london.gov.uk/api/action/package_show"

# Manually selected dataset URLs mapped to keyword queries
gold_standard_datasets = {
    "population growth": [
        "https://data.london.gov.uk/dataset/2011-census-demography",
        "https://data.london.gov.uk/dataset/global-city-data",
        "https://data.london.gov.uk/dataset/population-change-1939-2015",
        "https://data.london.gov.uk/dataset/global-city-population-estimates",
        "https://data.london.gov.uk/dataset/gla-population-projections-custom-age-tables"
    ],
    "crime rate by borough": [
        "https://data.london.gov.uk/dataset/recorded_crime_summary",
        "https://data.london.gov.uk/dataset/mps-crime-statistics-financial-year-2022-23",
        "https://data.london.gov.uk/dataset/mps-monthly-crime-dahboard-data",
        "https://data.london.gov.uk/dataset/transport-crime-london?q=Crime%20rate%20",
        "https://data.london.gov.uk/dataset/mps-business-crime-dashboard-data"

    ],
    "air pollution levels": [
        "https://data.london.gov.uk/dataset/london-average-air-quality-levels",
        "https://data.london.gov.uk/dataset/analysing-air-pollution-exposure-in-london",
        "https://data.london.gov.uk/dataset/pm2-5-map-and-exposure-data",
        "https://data.london.gov.uk/dataset/air-quality-focus-areas",
        "https://data.london.gov.uk/dataset/london-atmospheric-emissions-inventory--laei--2016-air-quality-focus-areas",
        "https://data.london.gov.uk/dataset/breathe-london-mobile-monitoring",
        "https://data.london.gov.uk/dataset/london-atmospheric-emissions-inventory--laei--2019",
        "https://data.london.gov.uk/dataset/air-quality-monitoring-diffusion-tube-results",
        "https://data.london.gov.uk/dataset/laei-2013-london-focus-areas",
        "https://data.london.gov.uk/dataset/london-atmospheric-emissions-inventory--laei--2019-air-quality-focus-areas",
        "https://data.london.gov.uk/dataset/low_emission_neighbourhoods",
        "https://data.london.gov.uk/dataset/london-atmospheric-emissions-inventory-2010",
        "https://data.london.gov.uk/dataset/laei-2008",
        "https://data.london.gov.uk/dataset/air-quality-summary-statistics",
        "https://data.london.gov.uk/dataset/air_quality_monitoring_sites"
    ],
    "public transport usage": [
        "https://data.london.gov.uk/dataset/origin-and-destination-of-public-transport-journeys",
        "https://data.london.gov.uk/dataset/public-transport-journeys-type-transport",
        "https://data.london.gov.uk/dataset/public-transport-accessibility-levels",
        "https://data.london.gov.uk/dataset/bus-use-and-supply-data-1999-2022",
        "https://data.london.gov.uk/dataset/travel-patterns-and-trends-london",
        "https://data.london.gov.uk/dataset/global-city-data"
    ],
    "average house prices": [
        "https://data.london.gov.uk/dataset/monthly-mix-adjusted-average-house-prices-london?q=monthly%20mix%20",
        "https://data.london.gov.uk/dataset/house-price-per-square-metre-in-england-and-wales",
        "https://data.london.gov.uk/dataset/london-housing",
        "https://data.london.gov.uk/dataset/index-private-housing-rental-prices-region",
        "https://data.london.gov.uk/dataset/london-economy-today",
        "https://data.london.gov.uk/dataset/focus-on-london-housing",
        "https://data.london.gov.uk/dataset/travel-patterns-and-trends-london",
        "https://data.london.gov.uk/dataset/average-house-prices?q=average%20house%20prices%20",
        "https://data.london.gov.uk/dataset/index-private-housing-rental-prices-region?q=da%20private%20housing%20rental%20prices",
        "https://data.london.gov.uk/dataset/uk-house-price-index",
        "https://data.london.gov.uk/dataset/housing-sales"
    ],
    "unemployment statistics": [
        "https://data.london.gov.uk/dataset/unemployment-rate-region",
        "https://data.london.gov.uk/dataset/economic-activity-rate-employment-rate-and-unemployment-rate-ethnic-group-national",
        "https://data.london.gov.uk/dataset/unemployment-london-2012",
        "https://data.london.gov.uk/dataset/london-economy-today",
        "https://data.london.gov.uk/dataset/flows-into-and-out-of-employment",
        "https://data.london.gov.uk/dataset/dmag-briefings-2010",
        "https://data.london.gov.uk/dataset/dmag-briefings-2009",
        "https://data.london.gov.uk/dataset/dmag-briefings-2002",
        "https://data.london.gov.uk/dataset/labour-market-flows",
        "https://data.london.gov.uk/dataset/dmag-briefings-2003",
        "https://data.london.gov.uk/dataset/dmag-briefings-2007",
        "https://data.london.gov.uk/dataset/dmag-briefings-2004",
        "https://data.london.gov.uk/dataset/dmag-briefings-2004",
        "https://data.london.gov.uk/dataset/dmag-briefings-2008"
    ],
    "NHS waiting times": [
    ],
    "energy consumption trends": [
        "https://data.london.gov.uk/dataset/electricity-consumption-borough",
        "https://data.london.gov.uk/dataset/total-energy-consumption-borough?q=energy%20consumption",
        "https://data.london.gov.uk/dataset/road-transport-energy-consumption-borough?q=energy%20consumption",
        "https://data.london.gov.uk/dataset/smartmeter-energy-use-data-in-london-households",
        "https://data.london.gov.uk/dataset/leggi",
        "https://data.london.gov.uk/dataset/consumption-other-fuels-borough",
        "https://data.london.gov.uk/dataset/london-s-consumption-based-greenhouse-gas-emissions"
    ], 
    "green space data": [
        "https://data.london.gov.uk/dataset/green-infrastructure-focus-map",
        "https://data.london.gov.uk/dataset/green-and-blue-cover",
        "https://data.london.gov.uk/dataset/green-cover-2024",
        "https://data.london.gov.uk/dataset/area-designated-green-belt-land",
        "https://data.london.gov.uk/dataset/potential-woodland-creation-sites-in-london-s-green-belt"
    ], 
    "business startup rates": [
        "https://data.london.gov.uk/dataset/investment-in-start-up-and-scale-up-in-tech-sector",
        "https://data.london.gov.uk/dataset/business-demographics-and-survival-rates-borough",
        "https://data.london.gov.uk/dataset/london-business-survey-2014-business-profile"
    ], 
    "ethnic diversity": [
        "https://data.london.gov.uk/dataset/2011-census-diversity",
        "https://data.london.gov.uk/dataset/ethnic-groups-borough",
        "https://data.london.gov.uk/dataset/intelligence-unit-briefings-2013",
        "https://data.london.gov.uk/dataset/dmag-briefings-2005",
        "https://data.london.gov.uk/dataset/intelligence-unit-briefings-2012",
        "https://data.london.gov.uk/dataset/dmag-briefings-2003",
    	"https://data.london.gov.uk/dataset/dmag-briefings-2006",
        "https://data.london.gov.uk/dataset/dmag-briefings-2007"
    ],
    "road traffic accidents": [
        "https://data.london.gov.uk/dataset/road-casualties-severity-borough",
        "https://data.london.gov.uk/dataset/tfl-live-traffic-disruptions"
    ],
    "homelessness numbers": [
        "https://data.london.gov.uk/dataset/homelessness",
        "https://data.london.gov.uk/dataset/chain-reports",
        "https://data.london.gov.uk/dataset/public-health-outcomes-framework-indicators"
    ],
    "broadband coverage": [
        "https://data.london.gov.uk/dataset/ofcom-fixed-broadband-speeds"
    ],
    "waste recycling rates": [
        "https://data.london.gov.uk/dataset/household-waste-recycling-rates-borough",
        "https://data.london.gov.uk/dataset/gla-poll-results-2009"
    ],
    "tourist visits": [
        "https://data.london.gov.uk/dataset/number-international-visitors-london",
        "https://data.london.gov.uk/dataset/tourism-trips-borough",
        "https://data.london.gov.uk/dataset/global-city-data",
        "https://data.london.gov.uk/dataset/london-tourism-forecasts",
        "https://data.london.gov.uk/dataset/daytime-population-borough",
        "https://data.london.gov.uk/dataset/tourism-spend-estimates"
    ],
    "school performance data": [
        "https://data.london.gov.uk/dataset/gcse-results-by-borough",
        "https://data.london.gov.uk/dataset/further-education-and-higher-education-destinations-ks5-students-borough-and-insti",
        "https://data.london.gov.uk/dataset/key-stage-1-results-by-borough",
        "https://data.london.gov.uk/dataset/key-stage-2-results-by-borough"
    ],
    "CO2 emissions": [
        "https://data.london.gov.uk/dataset/london-atmospheric-emissions-inventory--laei--2019",
        "https://data.london.gov.uk/dataset/leggi",
        "https://data.london.gov.uk/dataset/london-atmospheric-emissions-inventory-2013",
        "https://data.london.gov.uk/dataset/london-atmospheric-emissions-inventory--laei--2016",
        "https://data.london.gov.uk/dataset/climate-change-mitigation-and-energy-annual-report-2009-2012-data",
        "https://data.london.gov.uk/dataset/ccme-annual-report-data-2013-14",
        "https://data.london.gov.uk/dataset/laei-2016---borough-air-quality-data-for-llaqm"

    ],
    "traffic congestion": [
        "https://data.london.gov.uk/dataset/traffic-flows-borough",
        "https://data.london.gov.uk/dataset/tfl-live-traffic-cameras",
        "https://data.london.gov.uk/dataset/tfl-live-traffic-disruptions"
    ],
    "obesity rates": [
        "https://data.london.gov.uk/dataset/obesity-adults",
        "https://data.london.gov.uk/dataset/prevalence-childhood-obesity-borough",
        "https://data.london.gov.uk/dataset/healthy-lifestyle-behaviours",
        "https://data.london.gov.uk/dataset/focus-on-london-health",
        "https://data.london.gov.uk/dataset/london-ward-well-being-scores"
    ]
}

def extract_dataset_id(dataset_url):
    """Extracts dataset ID from URL."""
    match = re.search(r'dataset/([^?/]+)', dataset_url)
    return match.group(1) if match else None

def fetch_dataset_info(dataset_url, retries=3):
    """Fetch dataset details including title, description, and resources from the London Data API."""
    dataset_id = extract_dataset_id(dataset_url)
    if not dataset_id:
        print(f"⚠️ Invalid dataset URL format: {dataset_url}")
        return None

    params = {"id": dataset_id}

    for attempt in range(retries):
        try:
            response = requests.get(BASE_URL, params=params, timeout=10)

            if response.status_code == 200:
                data = response.json()
                
                if not data.get("success", False) or "result" not in data:
                    print(f"⚠️ Unexpected response structure for: {dataset_url}")
                    return None

                result = data["result"]

                # Extract dataset title and description
                dataset_title = result.get("title") or result.get("name") or "No Title Available"
                dataset_description = result.get("notes") or result.get("summary", "No description available.")

                dataset_entry = {
                    "title": dataset_title,
                    "description": dataset_description,  # Now extracting descriptions
                    "dataset_page": dataset_url,
                    "resources": []
                }

                # Extract resources (files)
                for resource in result.get("resources", []):
                    resource_id = resource.get("id", "")
                    resource_name = resource.get("name", "Unnamed Resource")
                    resource_format = resource.get("format", "")

                    # Encode filename properly
                    encoded_filename = urllib.parse.quote(resource_name)

                    # Construct correct resource URL
                    resource_url = f"https://data.london.gov.uk/download/{dataset_id}/{resource_id}/{encoded_filename}"

                    dataset_entry["resources"].append({
                        "name": resource_name,
                        "url": resource_url,
                        "format": resource_format,
                    })

                return dataset_entry

            else:
                print(f"❌ API Error {response.status_code} for {dataset_url}")

        except requests.exceptions.RequestException as e:
            print(f"⏳ Attempt {attempt + 1}: Request failed due to {e}")
            time.sleep(2)  # Wait before retrying

    print(f"🚫 Failed to fetch data after {retries} retries for {dataset_url}")
    return None

# Fetch and compile ground truth data
ground_truth_data = []

for keyword, dataset_urls in gold_standard_datasets.items():
    print(f"🔎 Processing: {keyword}")

    retrieved_datasets = []
    for dataset_url in dataset_urls:
        dataset_info = fetch_dataset_info(dataset_url)
        if dataset_info:
            retrieved_datasets.append(dataset_info)

    ground_truth_data.append({
        "keyword_search": keyword,
        "retrieved_datasets": retrieved_datasets
    })

# Save structured data to JSON file
output_filename = "ground_truth.json"
with open(output_filename, "w", encoding="utf-8") as f:
    json.dump(ground_truth_data, f, indent=4, ensure_ascii=False)

print(f"✅ Ground truth dataset saved as '{output_filename}'")