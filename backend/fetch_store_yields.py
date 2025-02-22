import os
import xml.etree.ElementTree as ET
from datetime import datetime

import psycopg2
import requests

DATABASE_URL = os.getenv("DATABASE_URL")


def fetch_and_store_yields():
    url = "https://home.treasury.gov/resource-center/data-chart-center/interest-rates/pages/xml?data=daily_treasury_yield_curve&field_tdr_date_value=all&page=0"

    response = requests.get(url)
    if response.status_code != 200:
        print("❌ Failed to fetch yield data")
        return

    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()

    for entry in parse_xml(response.text):  # You should have a function that parses XML
        try:
            cur.execute("""
                INSERT INTO bond_yields (date, yield_3m, yield_6m, yield_1y, yield_2y, yield_3y, yield_5y, yield_7y, yield_10y, yield_30y)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (date) DO NOTHING;
            """, entry)
        except Exception as e:
            print(f"Error inserting: {e}")

    conn.commit()
    cur.close()
    conn.close()
    print("✅ Yield curve data stored successfully!")


def parse_xml(xml_data):
    """
    Parses the Treasury Yield Curve XML data and extracts relevant bond yields.

    Args:
        xml_data (str): XML response text.

    Returns:
        list of tuples: Each tuple contains (date, yield_3m, yield_6m, yield_1y, yield_2y, yield_3y, yield_5y, yield_7y, yield_10y, yield_30y)
    """
    root = ET.fromstring(xml_data)
    namespace = {"d": "http://schemas.microsoft.com/ado/2007/08/dataservices",
                 "m": "http://schemas.microsoft.com/ado/2007/08/dataservices/metadata"}

    parsed_data = []

    for entry in root.findall(".//entry", namespaces=namespace):
        properties = entry.find(".//m:properties", namespaces=namespace)

        try:
            date_str = properties.find("d:NEW_DATE", namespaces=namespace).text
            date = datetime.strptime(date_str[:10], "%Y-%m-%d").date()

            yield_3m = float(properties.find("d:BC_3MONTH", namespaces=namespace).text or 0)
            yield_6m = float(properties.find("d:BC_6MONTH", namespaces=namespace).text or 0)
            yield_1y = float(properties.find("d:BC_1YEAR", namespaces=namespace).text or 0)
            yield_2y = float(properties.find("d:BC_2YEAR", namespaces=namespace).text or 0)
            yield_3y = float(properties.find("d:BC_3YEAR", namespaces=namespace).text or 0)
            yield_5y = float(properties.find("d:BC_5YEAR", namespaces=namespace).text or 0)
            yield_7y = float(properties.find("d:BC_7YEAR", namespaces=namespace).text or 0)
            yield_10y = float(properties.find("d:BC_10YEAR", namespaces=namespace).text or 0)
            yield_30y = float(properties.find("d:BC_30YEAR", namespaces=namespace).text or 0)

            parsed_data.append(
                (date, yield_3m, yield_6m, yield_1y, yield_2y, yield_3y, yield_5y, yield_7y, yield_10y, yield_30y))

        except Exception as e:
            print(f"❌ Error parsing XML entry: {e}")

    return parsed_data


if __name__ == "__main__":
    fetch_and_store_yields()
