import pandas as pd
from sqlalchemy import create_engine, text
import openai
import json
from urllib.parse import quote_plus
from sqlalchemy.engine import URL

def load_config(json_path="/root/ysl_project_clean/ysl_config.json"):
    with open(json_path, "r", encoding="utf-8") as f:
        return json.load(f)

def read_from_postgres(table_name=None, query=None):
    db = load_config()['db_connection']
    engine = create_engine(f'postgresql+psycopg2://{db["user"]}:{db["password"]}@{db["host"]}:{db["port"]}/{db["dbname"]}')
    if query:
        df = pd.read_sql_query(query, engine)
    else:
        df = pd.read_sql_table(table_name, engine)
    return df

def query_to_new_table(query, new_table_name):
    db = load_config()['db_connection']
    engine = create_engine(
        f'postgresql+psycopg2://{db["user"]}:{db["password"]}@{db["host"]}:{db["port"]}/{db["dbname"]}'
    )

    drop_sql = f"DROP TABLE IF EXISTS {new_table_name};"
    create_sql = f"CREATE TABLE {new_table_name} AS {query};"
    full_sql = drop_sql + create_sql
    
    with engine.connect() as conn:
        conn.execution_options(isolation_level="AUTOCOMMIT").execute(text(full_sql))

def write_to_postgres(df, table_name, mode="overwrite", batch_size=10000):
    """
    mode: 'append' | 'overwrite'
    """
    cfg = load_config("/root/ysl_project_clean/ysl_config.json")["db_connection"]

    # ✅ 用 URL.create 处理密码中的特殊字符（#、@、% 等）
    url = URL.create(
        drivername="postgresql+psycopg2",
        username=cfg["user"],
        password=cfg["password"],
        host=cfg["host"],
        port=int(cfg["port"]),
        database=cfg["dbname"]
    )

    engine = create_engine(url, pool_pre_ping=True, future=True)

    schema = table_name.split(".")[0] if "." in table_name else None
    tbl = table_name.split(".")[-1]

    if mode == "overwrite":
        df.to_sql(
            name=tbl,
            schema=schema,
            con=engine,
            if_exists="replace",
            index=False,
            method="multi",
            chunksize=batch_size,
        )
    elif mode == "append":
        df.to_sql(
            name=tbl,
            schema=schema,
            con=engine,
            if_exists="append",
            index=False,
            method="multi",
            chunksize=batch_size,
        )
    else:
        raise ValueError("mode 必须是 'append' 或 'overwrite'")

    print(f"✅ 成功写入 {len(df)} 行到表 {table_name}")