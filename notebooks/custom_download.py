import pandas as pd
import clickhouse_connect
import builtins
import re
from datetime import datetime

import os
from dotenv import load_dotenv
CLICKHOUSE_DB = 'card_data'
CLICKHOUSE_TABLE = 'medium_transactions'
path_data = '../data/HI-Medium_Patterns.txt'

load_dotenv("../.env")  
CH_HOST = os.getenv('CH_HOST')
CH_PORT = os.getenv('CH_PORT') 
CH_USER = os.getenv('CH_USER')
CH_PASSWORD = os.getenv('CH_PASSWORD')

import clickhouse_connect

client = clickhouse_connect.get_client(host=CH_HOST, port=CH_PORT, user=CH_USER, password=CH_PASSWORD)



def create_client():
    client = clickhouse_connect.get_client(
        host=CH_HOST,
        port=CH_PORT,
        user=CH_USER,
        password=CH_PASSWORD
    )
    return client


def full_func_creation_table(df, db, table):
    null_counts = df.isnull().sum().to_dict()
    total_nulls = builtins.sum(null_counts.values())

    types = {
        'object': 'String',
        'int64': 'Int64',
        'float64': 'Float64',
        'bool': 'UInt8',
        'datetime64[ns]': 'DateTime'
    }

    columns = [
        f"{col} {types.get(str(dtype), 'String')}"
        for col, dtype in df.dtypes.items()
    ]

    return f"CREATE TABLE IF NOT EXISTS {db}.{table} ({', '.join(columns)}) ENGINE = MergeTree() ORDER BY {df.columns[0]}"


def loading_to_db(client, ddl, dataframe, db, table):
    client.command(f"DROP TABLE IF EXISTS {db}.{table}")
    client.command(ddl)
    client.insert_df(f"{db}.{table}", dataframe)


def creation_table(path_data):
    with open(path_data, 'r') as f:
        lines = f.readlines()

    data = []
    suspicious_type = None

    for line in lines:
        line = line.strip()

        if line.startswith("BEGIN LAUNDERING ATTEMPT"):
            match = re.search(r"BEGIN LAUNDERING ATTEMPT - (.*)", line)
            if match:
                suspicious_type = match.group(1)
            continue

        if line.startswith("END LAUNDERING ATTEMPT"):
            continue

        split_values = line.split(',')
        if len(split_values) < 11:
            continue 

        timestamp = split_values[0]
        if not re.match(r'^\d{4}/\d{2}/\d{2} \d{2}:\d{2}', timestamp):
            continue

        try:
            row = {
                'Timestamp': datetime.strptime(timestamp, '%Y/%m/%d %H:%M'),
                'From_ID': split_values[1],
                'To_ID': split_values[3],
                'To_Account': split_values[4],
                'Amount': float(split_values[5]),
                'Currency': split_values[6],
                'Converted_Amount': float(split_values[7]),
                'Converted_Currency': split_values[8],
                'Transaction_Type': split_values[9],
                'Flag': split_values[10],
                'Suspicious_Type': suspicious_type
            }
            data.append(row)
        except Exception as e:
            continue  

    df = pd.DataFrame(data)
    return df


def main():
    client = create_client()
    df = creation_table()
    ddl = full_func_creation_table(df, CLICKHOUSE_DB, CLICKHOUSE_TABLE)
    loading_to_db(client, ddl, df, CLICKHOUSE_DB, CLICKHOUSE_TABLE)


if __name__ == "__main__":
    main()
