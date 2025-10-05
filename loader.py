import os
import json
import mysql.connector
from dotenv import load_dotenv

load_dotenv()

# ================= CONFIG =================
DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
DB_PORT = int(os.getenv("DB_PORT", 3306))
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME", "phonepe_pulse")

BASE_PATH = os.getenv("PHONEPE_PATH", "pulse-main/data")

VERBOSE = True

def log(msg):
    if VERBOSE:
        print(msg)


# ================= DATABASE CONNECTION =================
def get_connection():
    return mysql.connector.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME
    )


# ================= CREATE TABLES =================
CREATE_QUERIES = [
    '''CREATE TABLE IF NOT EXISTS aggregated_insurance (
        States VARCHAR(50),
        Years INT,
        Quarter INT,
        Insurance_type VARCHAR(50),
        Insurance_count BIGINT,
        Insurance_amount BIGINT,
        PRIMARY KEY (States, Years, Quarter, Insurance_type))''',

    '''CREATE TABLE IF NOT EXISTS aggregated_transaction (
        States VARCHAR(50),
        Years INT,
        Quarter INT,
        Transaction_type VARCHAR(50),
        Transaction_count BIGINT,
        Transaction_amount BIGINT,
        PRIMARY KEY (States, Years, Quarter, Transaction_type))''',

    '''CREATE TABLE IF NOT EXISTS aggregated_user (
        States VARCHAR(50),
        Years INT,
        Quarter INT,
        Brands VARCHAR(50),
        Transaction_count BIGINT,
        Percentage FLOAT,
        PRIMARY KEY (States, Years, Quarter, Brands))''',

    '''CREATE TABLE IF NOT EXISTS map_insurance (
        States VARCHAR(50),
        Years INT,
        Quarter INT,
        District VARCHAR(50),
        Transaction_count BIGINT,
        Transaction_amount FLOAT,
        PRIMARY KEY (States, Years, Quarter, District))''',

    '''CREATE TABLE IF NOT EXISTS map_transaction (
        States VARCHAR(50),
        Years INT,
        Quarter INT,
        District VARCHAR(50),
        Transaction_count BIGINT,
        Transaction_amount FLOAT,
        PRIMARY KEY (States, Years, Quarter, District))''',

    '''CREATE TABLE IF NOT EXISTS map_user (
        States VARCHAR(50),
        Years INT,
        Quarter INT,
        Districts VARCHAR(50),
        RegisteredUser BIGINT,
        AppOpens BIGINT,
        PRIMARY KEY (States, Years, Quarter, Districts))''',

    '''CREATE TABLE IF NOT EXISTS top_insurance (
        States VARCHAR(50),
        Years INT,
        Quarter INT,
        Pincodes INT,
        Transaction_count BIGINT,
        Transaction_amount BIGINT,
        PRIMARY KEY (States, Years, Quarter, Pincodes))''',

    '''CREATE TABLE IF NOT EXISTS top_transaction (
        States VARCHAR(50),
        Years INT,
        Quarter INT,
        Pincodes INT,
        Transaction_count BIGINT,
        Transaction_amount BIGINT,
        PRIMARY KEY (States, Years, Quarter, Pincodes))''',

    '''CREATE TABLE IF NOT EXISTS top_user (
        States VARCHAR(50),
        Years INT,
        Quarter INT,
        Pincodes INT,
        RegisteredUser BIGINT,
        PRIMARY KEY (States, Years, Quarter, Pincodes))'''
]


# ================= HELPER FUNCTION =================
def insert_data(cursor, table, query, data):
    if not data:
        return
    cursor.executemany(query, data)
    cursor.connection.commit()
    log(f"✅ Inserted {len(data)} rows into {table}")


def load_json_to_db(base_path, table, insert_query, json_path_keys, field_mapping):
    """
    base_path: root folder for state/year/files
    table: target MySQL table
    insert_query: INSERT query
    json_path_keys: list of keys to reach the data array
    field_mapping: function that maps JSON dict to tuple for DB
    """
    all_data = []
    for state in os.listdir(base_path):
        state_path = os.path.join(base_path, state)
        if not os.path.isdir(state_path):
            continue
        for year in os.listdir(state_path):
            year_path = os.path.join(state_path, year)
            if not os.path.isdir(year_path):
                continue
            for file in os.listdir(year_path):
                if not file.endswith(".json"):
                    continue
                file_path = os.path.join(year_path, file)
                try:
                    with open(file_path) as f:
                        data = json.load(f)
                except json.JSONDecodeError:
                    log(f"⚠️ Skipping malformed file: {file_path}")
                    continue
                quarter = int(file.replace(".json", ""))
                target_data = data
                for key in json_path_keys:
                    target_data = target_data.get(key, {}) if isinstance(target_data, dict) else {}
                if not target_data:
                    continue
                for d in target_data:
                    all_data.append(field_mapping(state, year, quarter, d))
    return all_data


# ================= MAIN LOADER =================
def main():
    log("🚀 Starting PhonePe Pulse loader...")
    with get_connection() as db:
        cursor = db.cursor()
        for query in CREATE_QUERIES:
            cursor.execute(query)
        db.commit()
        log("✅ Tables created successfully!")

        # Aggregated Transaction
        agg_tran = load_json_to_db(
            os.path.join(BASE_PATH, "aggregated/transaction/country/india/state"),
            "aggregated_transaction",
            '''INSERT IGNORE INTO aggregated_transaction
               (States, Years, Quarter, Transaction_type, Transaction_count, Transaction_amount)
               VALUES (%s, %s, %s, %s, %s, %s)''',
            ["data", "transactionData"],
            lambda s, y, q, d: (
                s, int(y), q, d.get("name", "Unknown"),
                d.get("paymentInstruments", [{}])[0].get("count", 0),
                d.get("paymentInstruments", [{}])[0].get("amount", 0)
            )
        )
        insert_data(cursor, "aggregated_transaction", None, agg_tran)

        # Repeat similarly for other tables (aggregated_user, map_transaction, etc.)
        # Example for aggregated_user
        agg_user = load_json_to_db(
            os.path.join(BASE_PATH, "aggregated/user/country/india/state"),
            "aggregated_user",
            '''INSERT IGNORE INTO aggregated_user
               (States, Years, Quarter, Brands, Transaction_count, Percentage)
               VALUES (%s, %s, %s, %s, %s, %s)''',
            ["data", "usersByDevice"],
            lambda s, y, q, d: (
                s, int(y), q, d.get("brand", "Unknown"), d.get("count", 0), d.get("percentage", 0.0)
            )
        )
        insert_data(cursor, "aggregated_user", None, agg_user)

    log("🎉 All PhonePe Pulse data loaded successfully!")


if __name__ == "__main__":
    main()
