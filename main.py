import csv
import os
import psycopg2
from psycopg2 import sql
import pandas as pd
from config import config

# # --- Configuration ---
DB_CONFIG = config()

DATA_DIR = './data'
SCHEMA_FILE = os.path.join(DATA_DIR, 'INFORMATION_SCHEMA.csv')
#
# # --- Helper Functions ---
def connect_db():
    return psycopg2.connect(**DB_CONFIG)

def parse_schema(schema_path):
    df = pd.read_csv(schema_path)
    table_defs = {}
    for _, row in df.iterrows():
        table = row['table_name']
        column = row['column_name']
        datatype = row['data_type']
        if table not in table_defs:
            table_defs[table] = []
        table_defs[table].append((column, datatype))
    return table_defs

def create_tables(conn, table_defs):
    with conn.cursor() as cur:
        for table, columns in table_defs.items():
            col_defs = [f"{col} {dtype}" for col, dtype in columns]
            create_stmt = sql.SQL("CREATE TABLE IF NOT EXISTS {} ({});").format(
                sql.Identifier(table),
                sql.SQL(', ').join(sql.SQL(c) for c in col_defs)
            )
            cur.execute(create_stmt)
    conn.commit()

def insert_csv_to_table(conn, table_name, file_path):
    df = pd.read_csv(file_path)
    cols = list(df.columns)
    with conn.cursor() as cur:
        for _, row in df.iterrows():
            values = [row[col] if pd.notnull(row[col]) else None for col in cols]
            insert_stmt = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
                sql.Identifier(table_name),
                sql.SQL(', ').join(map(sql.Identifier, cols)),
                sql.SQL(', ').join(sql.Placeholder() * len(cols))
            )
            try:
                cur.execute(insert_stmt, values)
            except Exception as e:
                print(f"Failed to insert into {table_name}: {e}")
    conn.commit()

# --- Main ETL Workflow ---
def main():
    conn = connect_db()
    try:
        table_defs = parse_schema(SCHEMA_FILE)
        create_tables(conn, table_defs)

        for csv_file in os.listdir(DATA_DIR):
            if csv_file == 'INFORMATION_SCHEMA.csv' or not csv_file.endswith('.csv'):
                continue
            table_name = csv_file.replace('.csv', '')
            file_path = os.path.join(DATA_DIR, csv_file)
            print(f"Inserting data from {csv_file} into {table_name}...")
            insert_csv_to_table(conn, table_name, file_path)

    finally:
        conn.close()

if __name__ == '__main__':
    main()
