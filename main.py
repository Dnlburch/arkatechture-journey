import csv
import os
import psycopg2
from psycopg2 import sql
import pandas as pd
from config import config

#analysis vars
overdrawn_accounts = []
overpaid_loans = []
asset_sum = 0

# # --- Configuration ---
DB_CONFIG = config()

DATA_DIR = './data'
SCHEMA_FILE = os.path.join(DATA_DIR, 'INFORMATION_SCHEMA.csv')
#
# # --- Helper Functions ---
def connect_db():
    return psycopg2.connect(**DB_CONFIG)

# --- Phase 2 Analysis Functions ---
def get_overdrawn_checking_accounts(conn):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT c.account_guid,
                   c.starting_balance,
                   COALESCE(SUM(t.transaction_amount), 0) AS total_transactions,
                   c.starting_balance + COALESCE(SUM(t.transaction_amount), 0) AS ending_balance
            FROM checking c
            LEFT JOIN transactions t
            ON c.account_guid = t.account_guid
            GROUP BY c.account_guid, c.starting_balance
            HAVING (c.starting_balance + COALESCE(SUM(t.transaction_amount), 0)) < 0
        """)
        rows = cur.fetchall()
        print("[RESULT] Overdrawn Checking Accounts (calculated with transactions):")
        for account_guid, starting_balance, total_transactions, ending_balance in rows:
            print(f"- {account_guid}: Starting ${starting_balance:.2f}, Transactions ${total_transactions:.2f}, Ending ${ending_balance:.2f}")


def get_overpaid_loans(conn):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT l.account_guid,
                   l.starting_debt,
                   COALESCE(SUM(t.transaction_amount), 0) AS total_payments,
                   l.starting_debt + COALESCE(SUM(t.transaction_amount), 0) AS loan_balance
            FROM loans l
            LEFT JOIN transactions t
            ON l.account_guid = t.account_guid
            GROUP BY l.account_guid, l.starting_debt
            HAVING (l.starting_debt + COALESCE(SUM(t.transaction_amount), 0)) < 0
        """)
        rows = cur.fetchall()
        print("[RESULT] Overpaid Loans (calculated with transactions):")
        for account_guid, starting_debt, total_payments, loan_balance in rows:
            print(f"- {account_guid}: Starting ${starting_debt:.2f}, Payments ${total_payments:.2f}, Balance ${loan_balance:.2f}")

def get_total_asset_size(conn):
    with conn.cursor() as cur:
        try:
            cur.execute("""
                WITH checking_balances AS (
                    SELECT c.account_guid,
                           c.starting_balance + COALESCE(SUM(t.transaction_amount), 0) AS balance
                    FROM checking c
                    LEFT JOIN transactions t ON c.account_guid = t.account_guid
                    GROUP BY c.account_guid, c.starting_balance
                ),
                loan_balances AS (
                    SELECT l.account_guid,
                           l.starting_debt + COALESCE(SUM(t.transaction_amount), 0) AS balance
                    FROM loans l
                    LEFT JOIN transactions t ON l.account_guid = t.account_guid
                    GROUP BY l.account_guid, l.starting_debt
                )
                SELECT COALESCE(SUM(balance), 0) AS total_assets
                FROM (
                    SELECT balance FROM checking_balances
                    UNION ALL
                    SELECT balance FROM loan_balances
                ) all_balances
            """)
            total = cur.fetchone()[0]
            print(f"[RESULT] Total Asset Size of Institution: ${total:.2f}")
        except Exception as e:
            conn.rollback()
            print(f"[ERROR] Failed to calculate total asset size: {e}")

#PHASE 1
#so we do lower case everywhere because if you don't, you have key errors (Not matching case) and its just easier to all lower
#we also had to learn this the hard way. But this was really good experience as I have never started a database and instance
#from scratch. This was really good to know. I think there was definitely a better way to parse then what I did.
#what I kept running into was key errors mainly dealing with the datatype column and the numeric(.. rows.

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

def get_table_columns(conn, table_name):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = %s
        """, (table_name.lower(),))
        return [row[0] for row in cur.fetchall()]

def insert_csv_to_table(conn, table_name, file_path):
    df = pd.read_csv(file_path)
    df.columns = [col.lower() for col in df.columns]
    table_columns = get_table_columns(conn, table_name)
    insert_cols = [col for col in table_columns if col in df.columns]
    missing_cols = [col for col in table_columns if col not in df.columns]
    extra_cols = [col for col in df.columns if col not in table_columns]

    if missing_cols:
        print(f"[INFO] {table_name}: Missing columns in CSV that exist in DB: {missing_cols}")
    if extra_cols:
        print(f"[INFO] {table_name}: Extra columns in CSV not in DB: {extra_cols}")

    successful_inserts = 0
    failed_inserts = 0

    with conn.cursor() as cur:
        for _, row in df.iterrows():
            values = [row.get(col, None) for col in insert_cols]
            insert_stmt = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
                sql.Identifier(table_name.lower()),
                sql.SQL(', ').join(map(sql.Identifier, insert_cols)),
                sql.SQL(', ').join(sql.Placeholder() * len(insert_cols))
            )
            try:
                cur.execute(insert_stmt, values)
                successful_inserts += 1
            except Exception as e:
                failed_inserts += 1
                print(f"[ERROR] Failed to insert row into {table_name}: {e}")
    conn.commit()
    print(f"[SUMMARY] {table_name}: {successful_inserts} rows inserted, {failed_inserts} failed.")

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

        # --- Phase 2 Execution ---
        get_overdrawn_checking_accounts(conn)
        get_overpaid_loans(conn)
        get_total_asset_size(conn)

    finally:
        conn.close()

if __name__ == '__main__':
    main()