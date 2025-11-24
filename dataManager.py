import os
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
from tqdm import tqdm
import argparse
from pathlib import Path
import pyarrow.parquet as pq
import pyarrow.dataset as ds
import duckdb
import json

def build_upsert_sql(cols, name, conflict):
    # Quote reserved keywords
    quoted_cols = []
    for c in cols:
        if c in ("to", "from"):  # reserved keywords
            quoted_cols.append(f'"{c}"')
        else:
            quoted_cols.append(c)

    
    set_clause = ", ".join(
        f'"{c}" = EXCLUDED."{c}"'
        for c in cols
        if c not in conflict
    )
    conflict_cols = ', '.join(conflict)
    return f"""
    INSERT INTO "{name}" ({', '.join(quoted_cols)})
    VALUES ({", ".join(["?"] * len(cols))})
    ON CONFLICT ({conflict_cols}) DO UPDATE
    SET {set_clause}
    """
def sql_create_table(
    table_name,
    columns,
    primary_key,
    if_not_exists=True
):
    """
    Generate CREATE TABLE SQL dynamically.

    Args:
        table_name: str - Name of the table
        columns: dict - {column_name: sql_type}
        primary_key: list - List of PK column names
        jsonb_column: str or None - Name of JSONB catch-all
        if_not_exists: bool - Add IF NOT EXISTS

    Returns:
        str - Safe SQL
    """
    # Quote table name
    safe_table = f'"{table_name}"'

    # Quote reserved keywords
    def quote_col(col):
        reserved = {"to", "from", "order", "group", "user", "select", "where"}
        return f'"{col}"' if col.lower() in reserved else col

    # Build column definitions
    col_defs = []
    for col_name, col_type in columns.items():
        col_defs.append(f"    {quote_col(col_name)} "+ f"{col_type}")

    # # Add JSONB column
    # if jsonb_column:
    #     col_defs.append(f"    {quote_col(jsonb_column)} JSONB DEFAULT '{{}}'")

    # Primary key
    if primary_key:
        pk_cols = ", ".join(quote_col(c) for c in primary_key)
        col_defs.append(f"    PRIMARY KEY ({pk_cols})")

    # Join
    columns_sql = ",\n".join(col_defs)
    exists_clause = "IF NOT EXISTS " if if_not_exists else ""

    return f"""
CREATE TABLE {exists_clause}{safe_table} (
{columns_sql}
);
""".strip()
recordTableFormat = {    'block_number':     ' BIGINT  NOT NULL',
    'tx_hash'  :         'TEXT    NOT NULL',
    'contract_address' : 'TEXT    NOT NULL',
    'event_type'   :    ' TEXT    NOT NULL',
    'event_index'  :     'INTEGER NOT NULL',

    "to"      :        'TEXT',
    "from"     :       'TEXT',
    'sender'     :       'TEXT',
    'event_data'  :      "JSON   DEFAULT '{}'" }
balanceFormat = {'block_number': 'BIGINT NOT NULL ', 'user_address': 'TEXT NOT NULL ', 'token': 'TEXT NOT NULL ', 'balance': 'NUMERIC NOT NULL '}
classifiedEventsFormat = {'block_number': 'BIGINT NOT NULL', 'tx_hash': 'TEXT NOT NULL','type': 'TEXT','amount': 'NUMERIC', 'senders': 'JSON', 'receivers': 'JSON', 'approvals': 'JSON' }

class DataManager:
    def __init__(self, path = './dbs/db.duckdb', init = False):
        self.con = duckdb.connect(path)
        self.tables = {}
        self.tables['RecordData'] = Table('RecordData', recordTableFormat,None, ['block_number','tx_hash', 'event_index'], self.con, init=init )
        self.tables['UserBalances'] = Table('UserBalances', balanceFormat,('user_address','token', 'block_number DESC'), None, self.con, needsUnique= False, init=init )
        self.tables['ClassifiedEvents'] = Table("ClassifiedEvents", classifiedEventsFormat, None, ('block_number', 'tx_hash'), self.con, needsUnique = False, init=init)

class Table():   
    def __init__(self, name, tableFormat,  uniqueKeys, primaryKey, con,  init = False, needsUnique = True):
        self.name = name
        self.con = con
        self.columns = list(tableFormat.keys())
        self.tableFormat = tableFormat
        self.primaryKey = primaryKey
        self.uniqueKeys = uniqueKeys
        self.needsUnique = needsUnique
        if init:
            self.createTable()

    def createTable(self):
        try:
            self.con.execute("BEGIN TRANSACTION")
            print(sql_create_table(self.name, self.tableFormat,self.primaryKey ))                
            self.con.execute(sql_create_table(self.name, self.tableFormat,self.primaryKey ))
            if self.uniqueKeys:
                self.createIndex(self.name)
            self.con.execute("COMMIT")
            print("Table & index ready")
        except Exception as e:
            print(f"Table setup failed: {e}")
            self.con.execute("ROLLBACK")

    def createIndex(self, name):
        unique_cols = ", ".join(f'{col}' for col in self.uniqueKeys)
        index_name = f"idx_{name}_unique"
        unique_index = f"""
        CREATE {'UNIQUE' if self.needsUnique else ''} INDEX IF NOT EXISTS {index_name}
        ON "{self.name}" ({unique_cols});
        """.strip()
        print(unique_index) 
        self.con.execute(unique_index)
    
    def addTupleEvents(self, rows):
        # ---- upsert ----
        conflicts = list(self.primaryKey if self.primaryKey is not None else self.uniqueKeys)
        for i in range(len(conflicts)):
            if conflicts[i].strip().endswith(' DESC'):
                conflicts[i] = conflicts[i].strip()[:-5]
        upsert_sql = build_upsert_sql(self.columns, self.name,tuple(conflicts))
        try:
            # self.con.execute("BEGIN TRANSACTION")
            self.con.executemany(upsert_sql, rows)
            # self.con.execute("COMMIT")
        except Exception as e:
            self.con.execute("ROLLBACK")
            print(f"Insert failed: {e}")



    def fetch_events(
        self,
        block_range= None,
        tx_hash = None,
        contract_address = None,
        event_type = None,
        sender = None,
        limit = None, order = None
    ):
        query = f'SELECT * FROM "{self.name}"'
        conditions = []
        params = []
        def add_in_condition(field, values):
            if values is None:
                return
            # Normalize to tuple
            if isinstance(values, str):
                values = (values,)
            elif isinstance(values, (list, tuple)):
                values = tuple(values)
            else:
                raise ValueError(f"{field} must be str, list, or tuple")

            if len(values) == 1:
                conditions.append(f"{field} = ?")
                params.append(values[0])
            else:
                placeholders = ",".join(["?"] * len(values))
                conditions.append(f"{field} IN ({placeholders})")
                params.extend(values)
        if block_range:
            start, end = block_range
            conditions.append("block_number BETWEEN ? AND ?")
            params.extend([start, end])
        add_in_condition("tx_hash", tx_hash)
        add_in_condition("contract_address", contract_address)
        add_in_condition("event_type", event_type)
        add_in_condition("sender", sender)
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        if order:
            query += f" ORDER BY {','.join(c for c in order) }"
        if limit is not None:
            query += " LIMIT ?"
            params.append(limit)

        # Debug (uncomment to see SQL)
        # print(cur.mogrify(query, params).decode())

        self.con.execute(query, params)
        rows = self.con.fetchall()
        return rows
    
    def prepareJsonEvents(self, data):
        rows = []
        for block_str, block_data in data.items():
            block_number = int(block_str)
            for tx_hash, tx_data in block_data.items():
                for contract_address, pool_events in tx_data.items():
                    for event_name, payload in pool_events.items():
                        # ---- parse "Fees 169" → type + index ----
                        try:
                            event_type, idx_str = event_name.rsplit(" ", 1)
                            event_index = int(idx_str)
                        except Exception:
                            event_type = event_name
                            event_index = None
                        row = [None]*len(self.columns)
                        for i in range(len(self.columns)):
                            k = self.columns[i]
                            if k == 'block_number':
                                row[i] = block_number
                                continue
                            elif k == 'tx_hash':
                                row[i] = tx_hash
                                continue
                            elif k == 'contract_address':
                                row[i] = contract_address
                                continue
                            elif k == 'event_type':
                                row[i] = event_type
                                continue
                            elif k == 'event_index':
                                row[i] = event_index
                                continue
                            elif k in payload:
                                row[i] = payload[k]
                                del payload[k]
                                continue
                            elif k == 'event_data':
                                row[i] = json.dumps(payload) if payload else "{}"
                        rows.append(tuple(row))
        return rows

dm = DataManager()

# recordTable = Database("Record",  recordTableFormat, None, recordPrimaryKeys, 'block_number', pools)
# balanceTable = Database("UserBalances", balanceFormat, ('user_address','token', 'block_number DESC'),None, 'block_number', pools, needsUnique=False)
# tableStats = Database("TableStats", ['table', 'blocks'], balanceFormat,balanceColumns,('user_address','token', 'block_number DESC' ),None, 'block_number', pools, needsUnique=False)
# classifiedEvents = Database("ClassifiedEvents", classifiedEventsFormat, None, ('block_number', 'tx_hash'),  'block_number', pools, needsUnique = False, init=False)
#!/usr/bin/env python3
"""
Script to convert a directory of partitioned Parquet files into a single table in a DuckDB database file.

Usage:
    python convert_parquet_to_duckdb.py --input_dir /path/to/partitioned/parquets --output_db output.duckdb --table_name my_table

Requirements:
    pip install duckdb

This script uses DuckDB's `read_parquet` function to recursively load all Parquet files from the input directory
(assuming Hive-style partitioning like year=2023/month=1/file.parquet). It creates or replaces the specified table
in the output .duckdb file. Partition columns are automatically inferred from directory names if not present in the data.

Note: For very large datasets, this loads everything into the DuckDB file for optimal querying. If you want to keep
external storage, consider attaching the Parquet dir as a foreign table instead.
"""

import argparse
import duckdb
import os


# def convert_partitioned_parquet_to_duckdb(input_dir: str, output_db: str, table_name: str = "my_table"):
#     """
#     Convert partitioned Parquet directory to a DuckDB table.

#     Args:
#         input_dir (str): Path to the base directory containing partitioned Parquet files.
#         output_db (str): Path to the output .duckdb file (created if it doesn't exist).
#         table_name (str): Name of the table to create in the DuckDB file.
#     """
#     if not os.path.exists(input_dir):
#         raise ValueError(f"Input directory '{input_dir}' does not exist.")
#     if not os.path.exists(os.path.dirname(output_db)):
#         os.makedirs(os.path.dirname(output_db), exist_ok=True) 
#     # Connect to (or create) the persistent DuckDB file
#     con = duckdb.connect(output_db)

#     # Use glob pattern to recursively read all Parquet files in subdirectories
#     # This handles partitioned structures like input_dir/year=2023/month=1/*.parquet
#     glob_pattern = os.path.join(input_dir, "**", "*.parquet").replace("\\", "/")  # Cross-platform path handling

#     try:
#         con.execute(
#             f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM read_parquet('{glob_pattern}')"
#         )
#         print(f"Successfully created table '{table_name}' in '{output_db}' from '{input_dir}'.")
        
#         # Optional: Print row count for verification
#         row_count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
#         print(f"Total rows loaded: {row_count}")
        
#     except Exception as e:
#         print(f"Error during conversion: {e}")
#         raise
#     finally:
#         con.close()


# if __name__ == "__main__":
#     # parser = argparse.ArgumentParser(description="Convert partitioned Parquets to DuckDB")
#     # parser.add_argument("--input_dir", required=True, help="Path to partitioned Parquet directory")
#     # parser.add_argument("--output_db", required=True, help="Path to output .duckdb file")
#     # parser.add_argument("--table_name", default="my_table", help="Name of the table in DuckDB (default: my_table)")
    
#     # args = parser.parse_args()
#     name = 'ClassifiedEvents'
#     convert_partitioned_parquet_to_duckdb(f'./parquet_data/{name}', f'./dbs/db.duckdb', name)
         
# # db= DataManager()