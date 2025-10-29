# utils.py
import os
import json
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine.url import URL

# ✅ 让配置路径优先读环境变量，其次读你给的 ysl_config.json，最后兜底到以前的路径
def load_config(json_path=None):
    json_path = (
        json_path
        or os.environ.get("YSL_CONFIG", "/root/ysl_project/ysl_config.json")
        or "/root/amora_trend_detector/Experiments/utils/config.json"
    )
    with open(json_path, 'r') as f:
        return json.load(f)

def _mk_engine():
    db = load_config()['db_connection']
    # ✅ 建议显式 client_encoding 与 pool_pre_ping
    url = f'postgresql+psycopg2://{db["user"]}:{db["password"]}@{db["host"]}:{db["port"]}/{db["dbname"]}'
    engine = create_engine(url, pool_pre_ping=True, connect_args={"options": "-c client_encoding=utf8"})
    return engine

def read_from_postgres(table_name=None, query=None, schema=None):
    engine = _mk_engine()
    if query:
        return pd.read_sql_query(text(query), engine)
    else:
        # ✅ pandas.read_sql_table 也支持 schema
        return pd.read_sql_table(table_name, engine, schema=schema)

def query_to_new_table(query, new_table_name, schema=None):
    """
    将 SELECT 查询结果落一张新表（先 DROP 再 CREATE TABLE AS SELECT）。
    允许 schema，例如 schema='content_tagging', new_table_name='test_ping_v1'
    """
    engine = _mk_engine()
    full_name = f'{schema}."{new_table_name}"' if schema else f'"{new_table_name}"'
    drop_sql = f'DROP TABLE IF EXISTS {full_name};'
    create_sql = f'CREATE TABLE {full_name} AS {query};'
    with engine.connect() as conn:
        conn.execution_options(isolation_level="AUTOCOMMIT").execute(text(drop_sql))
        conn.execution_options(isolation_level="AUTOCOMMIT").execute(text(create_sql))

def write_to_postgres(
    df: pd.DataFrame,
    table_name: str,
    mode: str = 'overwrite',
    partition_col: str | None = None,
    batch_size: int = 10000,
    schema: str | None = None,
):
    """
    mode: 'append', 'overwrite', 'overwrite_partition'
    schema: PostgreSQL schema 名（如 'content_tagging'）。⚠️请用这个参数，而不是把 schema 拼进 table_name 里
    """

    if df is None or df.empty:
        raise ValueError("DataFrame 为空，未写入。")

    engine = _mk_engine()

    if mode == 'overwrite':
        # 整表替换
        df.to_sql(
            name=table_name,
            con=engine,
            schema=schema,
            if_exists='replace',
            index=False,
            method='multi',
            chunksize=batch_size,
        )

    elif mode == 'append':
        # 追加（不存在则创建）
        df.to_sql(
            name=table_name,
            con=engine,
            schema=schema,
            if_exists='append',
            index=False,
            method='multi',
            chunksize=batch_size,
        )

    elif mode == 'overwrite_partition':
        if not partition_col or partition_col not in df.columns:
            raise ValueError("使用 'overwrite_partition' 时必须提供有效的 partition_col。")

        # 1) 确保表存在（只建结构）
        df.head(0).to_sql(
            name=table_name,
            con=engine,
            schema=schema,
            if_exists='append',   # 若不存在则创建；存在则不动
            index=False
        )

        # 2) 删除将要覆盖的分区值（安全绑定参数，避免注入 & 类型错）
        partitions = pd.unique(df[partition_col].dropna())
        if len(partitions) > 0:
            # 构造 :p0, :p1, ... 占位符
            binds = {f"p{i}": v for i, v in enumerate(partitions)}
            in_clause = ", ".join([f":p{i}" for i in range(len(partitions))])

            # ✅ schema 安全引用：schema.table 用 SQLAlchemy 的 text + f-string 轻量处理
            full_name = f'{schema}."{table_name}"' if schema else f'"{table_name}"'
            delete_sql = text(f'DELETE FROM {full_name} WHERE "{partition_col}" IN ({in_clause});')

            with engine.begin() as conn:
                conn.execute(delete_sql, binds)

        # 3) 插入新数据
        df.to_sql(
            name=table_name,
            con=engine,
            schema=schema,
            if_exists='append',
            index=False,
            method='multi',
            chunksize=batch_size,
        )
    else:
        raise ValueError("Invalid mode. Choose from 'append', 'overwrite', or 'overwrite_partition'.')