import requests
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()


def extract_permits(limit: int = 50000) -> pd.DataFrame:
    """
    Extract NYC building permit data from the Socrata API.
    Returns a cleaned DataFrame with relevant columns.
    """
    url = "https://data.cityofnewyork.us/resource/ipu4-2q9a.json"

    params = {
        "$limit": limit,
        "$offset": 0,
        "$order": "issuance_date DESC"
    }

    response = requests.get(url, params=params)
    response.raise_for_status()
    data = response.json()

    print(f"Records extracted: {len(data)}")

    columns_to_keep = [
        'borough', 'zip_code', 'block', 'lot',
        'job_type', 'permit_type', 'permit_status',
        'filing_date', 'issuance_date', 'expiration_date',
        'work_type', 'residential', 'bldg_type',
        'owner_s_business_name', 'owner_s_business_type',
        'permittee_s_business_name', 'permittee_s_license_type',
        'gis_latitude', 'gis_longitude', 'gis_nta_name'
    ]

    df = pd.DataFrame(data)
    df = df[[c for c in columns_to_keep if c in df.columns]]

    return df


def save_raw(df: pd.DataFrame, output_path: str) -> None:
    """Save raw DataFrame to CSV checkpoint."""
    df.to_csv(output_path, index=False)
    print(f"Raw data saved: {df.shape[0]} rows, {df.shape[1]} columns")


if __name__ == "__main__":
    df = extract_permits()
    save_raw(df, "data/raw_permits.csv")