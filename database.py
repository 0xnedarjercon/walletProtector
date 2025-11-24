#!/usr/bin/env python3
"""
Uniswap events → TimescaleDB
- All indexed fields are present (NULL if missing)
- Duplicate events are overwritten
- No 'NoneType' encode error
"""
from asynceventScanner.helpers.SqlGenerators import sql_create_table, build_upsert_sql
import json
from decimal import Decimal
def adaptDec(dec):
    return str(dec)
def adaptDict(d):
    return json.dumps(d)
import sqlite3
sqlite3.register_adapter(Decimal, adaptDec)
sqlite3.register_adapter(dict, adaptDict)
conn = None
cur = None


recordPrimaryKeys = ['block_number','tx_hash', 'event_index']

class Database():
    def __init__(self, name,path, tableFormat,  uniqueKeys, primaryKey, hypertableIndexer, pool, init = False, needsUnique = True, forceDb = False, sql = False):
        self.name = name
        self.placeHolder = '%s'
        self.connection_pool = pool
        self.path = path
        self.con = None
        self.forceDb = forceDb
        self.columns = list(tableFormat.keys())
        self.hypertableIndexer = hypertableIndexer
        self.tableFormat = tableFormat
        self.primaryKey = primaryKey
        self.uniqueKeys = uniqueKeys
        self.needsUnique = needsUnique
        self.sql = sql
        self.conflicts = list(self.primaryKey if self.primaryKey is not None else self.uniqueKeys)
        for i in range(len(self.conflicts)):
            if self.conflicts[i].strip().endswith(' DESC'):
                self.conflicts[i] = self.conflicts[i].strip()[:-5]  
        if init:
            self.ensureCon()
            self.createTable()
    # lazy connection for sql
    def ensureCon(self):
        if not self.con:
            self.placeHolder = '?'
            self.con = sqlite3.connect(f"{path}{self.name}.db")
            print(f'connected to local db for {self.name}')
        return self.con
    #creates table based on params configured in instance
    def createTable(self):
        try:
            cur = self.con.cursor()
            print(sql_create_table(self.name, self.tableFormat,self.primaryKey ))
            cur.execute(sql_create_table(self.name, self.tableFormat,self.primaryKey ))
            if self.uniqueKeys:
                self.createIndex(self.name)
            self.con.commit()
            print("Table & hypertable ready")
        except Exception as e:
            print(f"Table setup failed: {e}")
            self.con.rollback()

    def deleteTable(self):
        with self.con.cursor() as cur:
            # Correct
            cur.execute(f'DROP TABLE "{self.name}" ')
        self.con.commit()
        self.con.close()

    #creates an index on the table, use for faster searches    
    def createIndex(self, name):
            unique_cols = ", ".join(f'{col}' for col in self.uniqueKeys)
            index_name = f"idx_{name}_unique"
            unique_index = f"""
            CREATE {'UNIQUE' if self.needsUnique else ''} INDEX IF NOT EXISTS {index_name}
            ON "{self.name}" ({unique_cols});
            """.strip()
            with self.con.cursor() as cur:  
                cur.execute(unique_index)
            self.con.commit()

    #takes json events and formats them for insertion into db
    def prepareJsonEvents(self, data):
        rows = []
        for block_str, block_data in data.items():
            block_number = int(block_str)
            for tx_hash, tx_data in block_data.items():
                for contract_address, pool_events in tx_data.items():
                    for event_name, payload in pool_events.items():
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
                        rows.append(row)
        return rows

    #fetches from db based on query, removes need to specify the table
    # vars can be put directly into the query as strings or passed with placeholders and params
    def fetch(self, query, params = None, columns = None ):
        self.ensureCon()            
        query = f'SELECT {columns if columns else '*'} FROM "{self.name}" WHERE {query}'
        cur = self.con.cursor()
        try:
            if self.placeHolder != '%s':
                query =query.replace('%s', self.placeHolder)
            cur.execute(query, params)
            rows = cur.fetchall()
        finally:
            cur.close()
        return rows

    #add s to the db from tuple format
    #must be entire row, in the order of columns
    def addTupleEvents(self, rows):
        self.ensureCon()
        # ---- upsert ----

        upsert_sql = build_upsert_sql(self.columns, self.name,tuple(self.conflicts) )
        try:
            cur = self.con.cursor()
            if self.placeHolder != '%s':
                upsert_sql = upsert_sql.replace('%s', '?')
            cur.executemany(upsert_sql, rows)
            self.con.commit()
        except Exception as e:
            print(f"Insert failed: {e}")
            self.con.rollback()
        finally:
            cur.close() 

    #exports specified parts of the db to parquet file
    def export_chunk(self, start_block, end_block, output_path):
        import pandas as pd
        query = f"""
            SELECT * FROM "{self.name}"
            WHERE block_number BETWEEN %s AND %s
            ORDER BY block_number
        """
        df = pd.read_sql(query, self.conn, params=(start_block, end_block))
        if df.empty:
            return 0
        df.to_parquet(output_path, compression='zstd', index=False)
        return len(df)
    
    #exports entire db to csv, no chunking needed as this is streamed and super fast
    def exportCsv(self, csv_path):
        import csv
        self.ensureCon()
        cur = self.con.cursor()
        cur.execute(f'select * from {self.name} order by block_number')
        with open(csv_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(list(self.columns))
            writer.writerows(cur)     # all rows — streaming!
        

COLUMNS = [
        "block_number", "tx_hash", "contract_address",
        "event_type", "event_index",
        "to", "from", "sender",
        "event_data"
    ]
DB_PARAMS = {
    "host": "localhost",
    "port": 5432,
    "dbname": "timedb",
    "user": "postgres",
}

# Columns that become top-level (NULL if missing)
INDEXED_EVENTDATA = ["to", "from", "sender", "value"]


import os
pools = None
path = './dataProcessing/record/'
balanceColumns = ['block_number', 'user_address', 'token', 'balance']
balanceFormat = {'block_number': 'BIGINT NOT NULL ', 'user_address': 'TEXT NOT NULL ', 'balance': 'TEXT NOT NULL '}

classifiedEventsFormat = {'block_number': 'BIGINT NOT NULL', 'tx_hash': 'TEXT NOT NULL','type': 'TEXT','amount': 'TEXT', 'senders': 'JSONB', 'receivers': 'JSONB', 'approvals': 'JSONB' }

balancePrimaryKeys = []
recordTable = Database("Record",path,  classifiedEventsFormat, None, recordPrimaryKeys, 'block_number', pools)
recordBalanceTable = Database("RecordBalances",path, balanceFormat, None,('user_address', 'block_number'), 'block_number', pools, needsUnique=True, init = True, sql = True)
# tableStats = Database("TableStats", ['table', 'blocks'], balanceFormat,balanceColumns,('user_address','token', 'block_number DESC' ),None, 'block_number', pools, needsUnique=False)
classifiedEvents = Database("ClassifiedEvents",path,  classifiedEventsFormat, None, ('block_number', 'tx_hash'),  'block_number', pools, needsUnique = False, init=False)

   