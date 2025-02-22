import xml.etree.ElementTree as ET
from datetime import datetime

import pandas as pd
import requests

# Treasury XML API URL
URL = "https://home.treasury.gov/resource-center/data-chart-center/interest-rates/pages/xml?data=daily_treasury_yield_curve&field_tdr_date_value=all"


def fetch_yield_data():
    """Fetch and parse yield curve data from the U.S. Treasury XML feed."""
    response = requests.get(URL)
    if response.status_code != 200:
        raise Exception(f"Failed to fetch data: {response.status_code}")

    # Parse XML content
    root = ET.fromstring(response.content)

    # Define XML namespace
    ns = {
        "atom": "http://www.w3.org/2005/Atom",
        "d": "http://schemas.microsoft.com/ado/2007/08/dataservices",
        "m": "http://schemas.microsoft.com/ado/2007/08/dataservices/metadata",
    }

    # Extract all yield curve entries
    entries = root.findall("atom:entry", ns)

    # List to store structured data
    records = []

    for entry in entries:
        properties = entry.find("atom:content/m:properties", ns)

        date_text = properties.find("d:NEW_DATE", ns).text
        date = datetime.strptime(date_text, "%Y-%m-%dT00:00:00").date()

        # Extract yield rates (if available)
        yield_3m = properties.find("d:BC_3MONTH", ns)
        yield_6m = properties.find("d:BC_6MONTH", ns)
        yield_1y = properties.find("d:BC_1YEAR", ns)
        yield_2y = properties.find("d:BC_2YEAR", ns)
        yield_3y = properties.find("d:BC_3YEAR", ns)
        yield_5y = properties.find("d:BC_5YEAR", ns)
        yield_7y = properties.find("d:BC_7YEAR", ns)
        yield_10y = properties.find("d:BC_10YEAR", ns)
        yield_30y = properties.find("d:BC_30YEAR", ns)

        records.append({
            "date": date,
            "yield_3m": float(yield_3m.text) if yield_3m is not None else None,
            "yield_6m": float(yield_6m.text) if yield_6m is not None else None,
            "yield_1y": float(yield_1y.text) if yield_1y is not None else None,
            "yield_2y": float(yield_2y.text) if yield_2y is not None else None,
            "yield_3y": float(yield_3y.text) if yield_3y is not None else None,
            "yield_5y": float(yield_5y.text) if yield_5y is not None else None,
            "yield_7y": float(yield_7y.text) if yield_7y is not None else None,
            "yield_10y": float(yield_10y.text) if yield_10y is not None else None,
            "yield_30y": float(yield_30y.text) if yield_30y is not None else None,
        })

    # Convert to Pandas DataFrame
    df = pd.DataFrame(records)

    return df


# Fetch and display the latest yield data
if __name__ == "__main__":
    yield_data = fetch_yield_data()
    print(yield_data.head())  # Show first few rows
