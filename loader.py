import os
import json
import mysql.connector

# ================= DATABASE CONNECTION =================
db = mysql.connector.connect(
    host="localhost",
    user="root",
    password="kishan21",
    database="phonepe_pulse"
)
cursor = db.cursor()

# ================= CREATE TABLES =================
create_queries = [
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

for query in create_queries:
    cursor.execute(query)
db.commit()
print("✅ Tables created successfully!")

# ================= HELPER FUNCTION =================
def insert_data(table, query, data):
    if not data:
        return
    cursor.executemany(query, data)
    db.commit()
    print(f"✅ Inserted {len(data)} rows into {table}")

# ================= 1. AGGREGATED TRANSACTION =================
base_path = "pulse-main/data/aggregated/transaction/country/india/state/"
agg_tran_data = []
for state in os.listdir(base_path):
    for year in os.listdir(os.path.join(base_path, state)):
        for file in os.listdir(os.path.join(base_path, state, year)):
            if file.endswith(".json"):
                with open(os.path.join(base_path, state, year, file)) as f:
                    data = json.load(f)
                    quarter = int(file.strip(".json"))
                    for d in data["data"]["transactionData"] or []:
                        name = d["name"]
                        count = d["paymentInstruments"][0]["count"]
                        amount = d["paymentInstruments"][0]["amount"]
                        agg_tran_data.append((state, year, quarter, name, count, amount))

insert_query_tran = '''INSERT IGNORE INTO aggregated_transaction
(States, Years, Quarter, Transaction_type, Transaction_count, Transaction_amount)
VALUES (%s, %s, %s, %s, %s, %s)'''
insert_data("aggregated_transaction", insert_query_tran, agg_tran_data)

# ================= 2. AGGREGATED USER =================
base_path = "pulse-main/data/aggregated/user/country/india/state/"
agg_user_data = []
for state in os.listdir(base_path):
    for year in os.listdir(os.path.join(base_path, state)):
        for file in os.listdir(os.path.join(base_path, state, year)):
            if file.endswith(".json"):
                with open(os.path.join(base_path, state, year, file)) as f:
                    data = json.load(f)
                    quarter = int(file.strip(".json"))
                    if data["data"]["usersByDevice"]:
                        for d in data["data"]["usersByDevice"]:
                            brand = d["brand"]
                            count = d["count"]
                            percentage = d["percentage"]
                            agg_user_data.append((state, year, quarter, brand, count, percentage))

insert_query_user = '''INSERT IGNORE INTO aggregated_user
(States, Years, Quarter, Brands, Transaction_count, Percentage)
VALUES (%s, %s, %s, %s, %s, %s)'''
insert_data("aggregated_user", insert_query_user, agg_user_data)

# ================= 3. MAP TRANSACTION =================
base_path = "pulse-main/data/map/transaction/hover/country/india/state/"
map_tran_data = []
for state in os.listdir(base_path):
    for year in os.listdir(os.path.join(base_path, state)):
        for file in os.listdir(os.path.join(base_path, state, year)):
            if file.endswith(".json"):
                with open(os.path.join(base_path, state, year, file)) as f:
                    data = json.load(f)
                    quarter = int(file.strip(".json"))
                    for d in data["data"]["hoverDataList"] or []:
                        district = d["name"]
                        count = d["metric"][0]["count"]
                        amount = d["metric"][0]["amount"]
                        map_tran_data.append((state, year, quarter, district, count, amount))

insert_query_map_tran = '''INSERT IGNORE INTO map_transaction
(States, Years, Quarter, District, Transaction_count, Transaction_amount)
VALUES (%s, %s, %s, %s, %s, %s)'''
insert_data("map_transaction", insert_query_map_tran, map_tran_data)

# ================= 4. MAP USER =================
base_path = "pulse-main/data/map/user/hover/country/india/state/"
map_user_data = []
for state in os.listdir(base_path):
    for year in os.listdir(os.path.join(base_path, state)):
        for file in os.listdir(os.path.join(base_path, state, year)):
            if file.endswith(".json"):
                with open(os.path.join(base_path, state, year, file)) as f:
                    data = json.load(f)
                    quarter = int(file.strip(".json"))
                    for district, info in data["data"]["hoverData"].items():
                        registered = info["registeredUsers"]
                        app_opens = info["appOpens"]
                        map_user_data.append((state, year, quarter, district, registered, app_opens))

insert_query_map_user = '''INSERT IGNORE INTO map_user
(States, Years, Quarter, Districts, RegisteredUser, AppOpens)
VALUES (%s, %s, %s, %s, %s, %s)'''
insert_data("map_user", insert_query_map_user, map_user_data)

# ================= 5. TOP TRANSACTION =================
base_path = "pulse-main/data/top/transaction/country/india/state/"
top_tran_data = []
for state in os.listdir(base_path):
    for year in os.listdir(os.path.join(base_path, state)):
        for file in os.listdir(os.path.join(base_path, state, year)):
            if file.endswith(".json"):
                with open(os.path.join(base_path, state, year, file)) as f:
                    data = json.load(f)
                    quarter = int(file.strip(".json"))
                    for d in data["data"]["pincodes"] or []:
                        pincode = d["entityName"]
                        count = d["metric"]["count"]
                        amount = d["metric"]["amount"]
                        top_tran_data.append((state, year, quarter, pincode, count, amount))

insert_query_top_tran = '''INSERT IGNORE INTO top_transaction
(States, Years, Quarter, Pincodes, Transaction_count, Transaction_amount)
VALUES (%s, %s, %s, %s, %s, %s)'''
insert_data("top_transaction", insert_query_top_tran, top_tran_data)

# ================= 6. TOP USER =================
base_path = "pulse-main/data/top/user/country/india/state/"
top_user_data = []
for state in os.listdir(base_path):
    for year in os.listdir(os.path.join(base_path, state)):
        for file in os.listdir(os.path.join(base_path, state, year)):
            if file.endswith(".json"):
                with open(os.path.join(base_path, state, year, file)) as f:
                    data = json.load(f)
                    quarter = int(file.strip(".json"))
                    for d in data["data"]["pincodes"] or []:
                        pincode = d["name"]
                        reg_user = d["registeredUsers"]
                        top_user_data.append((state, year, quarter, pincode, reg_user))

insert_query_top_user = '''INSERT IGNORE INTO top_user
(States, Years, Quarter, Pincodes, RegisteredUser)
VALUES (%s, %s, %s, %s, %s)'''
insert_data("top_user", insert_query_top_user, top_user_data)
# ================= 7. AGGREGATED INSURANCE =================
# ================= 7. AGGREGATED INSURANCE =================
base_path = "pulse-main/data/aggregated/insurance/country/india/state/"
agg_insur_data = []

for state in os.listdir(base_path):
    for year in os.listdir(os.path.join(base_path, state)):
        for file in os.listdir(os.path.join(base_path, state, year)):
            if file.endswith(".json"):
                with open(os.path.join(base_path, state, year, file)) as f:
                    data = json.load(f)
                    quarter = int(file.strip(".json"))
                    
                    # Use "transactionData" key instead of "insuranceData"
                    for d in data.get("data", {}).get("transactionData", []) or []:
                        ins_type = d.get("name", "Unknown")
                        count = d["paymentInstruments"][0]["count"] if d.get("paymentInstruments") else 0
                        amount = d["paymentInstruments"][0]["amount"] if d.get("paymentInstruments") else 0
                        agg_insur_data.append((state, year, quarter, ins_type, count, amount))

insert_query_agg_insur = '''INSERT IGNORE INTO aggregated_insurance
(States, Years, Quarter, Insurance_type, Insurance_count, Insurance_amount)
VALUES (%s, %s, %s, %s, %s, %s)'''
insert_data("aggregated_insurance", insert_query_agg_insur, agg_insur_data)


# ================= 8. MAP INSURANCE =================
base_path = "pulse-main/data/map/insurance/hover/country/india/state/"
map_insur_data = []
for state in os.listdir(base_path):
    for year in os.listdir(os.path.join(base_path, state)):
        for file in os.listdir(os.path.join(base_path, state, year)):
            if file.endswith(".json"):
                with open(os.path.join(base_path, state, year, file)) as f:
                    data = json.load(f)
                    quarter = int(file.strip(".json"))
                    if data["data"] and "hoverDataList" in data["data"]:
                        for d in data["data"]["hoverDataList"] or []:
                            district = d.get("name", "Unknown")
                            count = d["metric"][0]["count"] if d.get("metric") else 0
                            amount = d["metric"][0]["amount"] if d.get("metric") else 0
                            map_insur_data.append((state, year, quarter, district, count, amount))

insert_query_map_insur = '''INSERT IGNORE INTO map_insurance
(States, Years, Quarter, District, Transaction_count, Transaction_amount)
VALUES (%s, %s, %s, %s, %s, %s)'''
insert_data("map_insurance", insert_query_map_insur, map_insur_data)

# ================= 9. TOP INSURANCE =================
base_path = "pulse-main/data/top/insurance/country/india/state/"
top_insur_data = []
for state in os.listdir(base_path):
    for year in os.listdir(os.path.join(base_path, state)):
        for file in os.listdir(os.path.join(base_path, state, year)):
            if file.endswith(".json"):
                with open(os.path.join(base_path, state, year, file)) as f:
                    data = json.load(f)
                    quarter = int(file.strip(".json"))
                    if data["data"]:
                        for d in data["data"].get("pincodes", []) or []:
                            entity_name = d.get("entityName", "Unknown")
                            count = d["metric"]["count"] if d.get("metric") else 0
                            amount = d["metric"]["amount"] if d.get("metric") else 0
                            top_insur_data.append((state, year, quarter, entity_name, count, amount))

insert_query_top_insur = '''INSERT IGNORE INTO top_insurance
(States, Years, Quarter, Pincodes, Transaction_count, Transaction_amount)
VALUES (%s, %s, %s, %s, %s, %s)'''
insert_data("top_insurance", insert_query_top_insur, top_insur_data)

# ================= FINISH =================
cursor.close()
db.close()
print("🎉 All PhonePe Pulse data loaded successfully into MySQL!")
