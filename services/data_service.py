# third-party
from fastapi import HTTPException

from fastapi import UploadFile
import pandas as pd
import psycopg2

# project
from settings import DATABASE_URL, YEARS_RANGE


def process_upload_internal(file: UploadFile):
    # file reading with the pandas
    df = pd.read_csv(file.file, delimiter=";")

    # version definition
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()
    cur.execute("SELECT MAX(version) FROM project_data")
    max_version = cur.fetchone()[0] or 0
    new_version = max_version + 1

    # preparing data for bulk insert
    data_to_insert = []
    for _, row in df.iterrows():
        for year in range(*YEARS_RANGE):
            value = row[str(year)]
            if pd.notna(value):
                data_to_insert.append((row['code'], row['project'], year, value, new_version))

    # adding data to the DB using executemany
    insert_query = "INSERT INTO project_data (code, project, year, value, version) VALUES (%s, %s, %s, %s, %s)"
    cur.executemany(insert_query, data_to_insert)

    conn.commit()
    cur.close()
    conn.close()

    return {"status": "success"}


def fetch_and_aggregate_data_internal(version: int):
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()

    # check if the version exists in the DB
    cur.execute("SELECT EXISTS(SELECT 1 FROM project_data WHERE version = %s)", (version,))
    version_exists = cur.fetchone()[0]
    if not version_exists:
        cur.close()
        conn.close()
        raise HTTPException(status_code=500, detail="Version not found")

    # extracting data from DB
    cur.execute("SELECT code, project, year, value FROM project_data WHERE version = %s", (version,))
    rows = cur.fetchall()

    # converting data to DataFrame for ease of processing
    df = pd.DataFrame(rows, columns=["code", "project", "year", "value"])

    # data aggregation for non-terminal nodes
    aggregated_rows = []
    for code in df['code'].unique():
        if df[df['code'] == code]['value'].isna().all():  # checking if a node is non-terminal
            child_values = df[df['code'].str.startswith(code) & (df['code'] != code)]
            aggregated_values = child_values.groupby('year').sum()['value']
            for year, value in aggregated_values.items():
                aggregated_rows.append((code, "Aggregated", year, value))

    # adding aggregated values to a DataFrame
    df_aggregated = pd.DataFrame(aggregated_rows, columns=["code", "project", "year", "value"])
    df = pd.concat([df, df_aggregated])

    # forming the structure of input files
    csv_data = df.to_csv(index=False, sep=";")


    cur.close()
    conn.close()

    return {"data": csv_data}
