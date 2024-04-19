import json
from hccpy.hcc import HCCEngine
from sqlalchemy import create_engine, MetaData, Table, select, text
from sqlalchemy.orm import Session
from sqlalchemy import create_engine
import pandas as pd
from dotenv import load_dotenv
import os, psycopg2
from datetime import datetime

# Load environment variables from .env file
load_dotenv()

DATABASE_USER = os.getenv('DATABASE_USER')
DATABASE_PASSWORD = os.getenv('DATABASE_PASSWORD')
DATABASE_HOST = os.getenv('DATABASE_HOST')
DATABASE_PORT = os.getenv('DATABASE_PORT')
DATABASE_NAME = os.getenv('DATABASE_NAME')

def load_from_db(limit=None):
    pg_connection_dict = {
            'dbname': DATABASE_NAME,
            'user': DATABASE_USER,
            'password': DATABASE_PASSWORD,
            'port': DATABASE_PORT,
            'host': DATABASE_HOST
    }
    engine = psycopg2.connect(**pg_connection_dict)
    query = """SELECT beneficiaries.id as beneficiary_id,
       beneficiaries.gender,
       beneficiaries.birthdate,
       ARRAY_AGG(icd10_cm_code) AS icd10_cm_codes
FROM beneficiaries
LEFT JOIN claims
    ON beneficiaries.horizon_member_id = claims.horizon_member_id
LEFT JOIN diagnoses
    ON claims.id = diagnoses.claim_id
GROUP BY beneficiary_id
"""
    if limit:
        query += f" LIMIT {limit}"
    df = pd.read_sql(query, con=engine)
    return df


patient_data = load_from_db(limit=50)


def calculate_age(birthdate):
    """
    Calculate the age of a person given their birthdate.

    :param birthdate_str: A string representing the birthdate in 'YYYY-MM-DD' format
    :return: Integer representing the age
    """
    # Convert the birthdate string to a datetime object
    if not birthdate:
        return None

    # Get the current date
    today = datetime.today()

    # Calculate the age
    age = today.year - birthdate.year - ((today.month, today.day) < (birthdate.month, birthdate.day))

    return age


def normalize_gender(gender):
    if not gender:
        return None
    if gender.lower() == "male":
        return "M"
    elif gender.lower() == "female":
        return "F"


for index, row in patient_data.iterrows():
    diagnosis_codes = row["icd10_cm_codes"]
    age = calculate_age(row["birthdate"])
    sex = normalize_gender(row["gender"])
    args = {"medicaid": True}
    if age:
        args["age"] = age
    if sex:
        args["sex"] = sex
    if diagnosis_codes and any(diagnosis_codes):
        he = HCCEngine("28")
        rp = he.profile(diagnosis_codes, **args)
    else:
        he = HCCEngine("28")
        rp = he.profile([], **args)

        # print(json.dumps(rp, indent=2))
        print(args)
        print(rp["risk_score"])
