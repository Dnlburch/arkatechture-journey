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

#so we do lower case everywhere because if you don't, you have key errors (Not matching case) and its just easier to all lower

def parse_schema(schema_path):
    rows = []
    with open(schema_path, newline='', encoding='utf-8') as csvfile:
        reader = csv.reader(csvfile, delimiter=',', quotechar='"')
        raw_header = next(reader)
        if len(raw_header) == 1:
            headers = [h.strip().strip('"') for h in raw_header[0].split(',')]
        else:
            headers = [h.strip().strip('"') for h in raw_header]

        for row in reader:
            row = [r.strip() for r in row]
            if len(row) > len(headers):
                # Fix split data_type field
                fixed_row = row[:4] + [','.join(row[4:]).strip()]
                row = fixed_row
            if len(row) != len(headers):
                raise ValueError(f"Malformed row after correction: {row}")
            rows.append(row)

    df = pd.DataFrame(rows, columns=headers)
    table_defs = {}
    for _, row in df.iterrows():
        table = row['TABLE_NAME'].lower()
        column = row['COLUMN_NAME'].lower()
        datatype = row['DATA_TYPE']
        if table not in table_defs:
            table_defs[table] = []
        table_defs[table].append((column, datatype))
    return table_defs

def create_tables(conn, table_defs):
    with conn.cursor() as cur:
        for table, columns in table_defs.items():
            col_defs = [f"{col} {dtype.strip().strip('\"')}" for col, dtype in columns]
            try:
                create_stmt = sql.SQL("CREATE TABLE IF NOT EXISTS {} ({});").format(
                    sql.Identifier(table),
                    sql.SQL(', ').join(sql.SQL(c) for c in col_defs)
                )
                print(f"\n[DEBUG] Creating table {table} with SQL: {create_stmt.as_string(conn)}")
                cur.execute(create_stmt)
            except Exception as e:
                conn.rollback()
                print(f"[ERROR] Failed to create table {table}: {e}")
                continue
    conn.commit()

def insert_csv_to_table(conn, table_name, file_path):
    df = pd.read_csv(file_path)
    cols = [col.lower() for col in df.columns]
    df.columns = cols
    with conn.cursor() as cur:
        for _, row in df.iterrows():
            values = [row[col] if pd.notnull(row[col]) else None for col in cols]
            insert_stmt = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
                sql.Identifier(table_name.lower()),
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
