# /root/ysl_project/sample_load_example.py
import os
import pandas as pd
from sqlalchemy import text
from utils import load_config, write_to_postgres, read_from_postgres

# ✅ 明确配置文件（也可以 export YSL_CONFIG=/root/ysl_project/ysl_config.json）
os.environ.setdefault("YSL_CONFIG", "/root/ysl_project/ysl_config.json")

# 造一小批示例数据（含中文/重音）
df = pd.DataFrame([
    {"id": 1, "标题": "YSL 包包种草", "内容": "NIKI 小号 黑色，通勤很百搭", "总互动量": 123, "dt": "2025-10-29"},
    {"id": 2, "标题": "穿搭分享",   "内容": "LE 5 À 7 腋下包，复古优雅",   "总互动量": 456, "dt": "2025-10-29"},
    {"id": 3, "标题": "材质讨论",   "内容": "羊皮/做旧皮 哪个更耐用？",     "总互动量": 78,  "dt": "2025-10-29"},
])

schema = "content_tagging"        # ✅ 用你有权限的 schema
table  = "test_ping_v1"

# 1) 整表覆盖
write_to_postgres(df, table_name=table, mode="overwrite", schema=schema, batch_size=1000)

# 2) 读回校验
out = read_from_postgres(table_name=table, schema=schema)
print(out.head())
print(f"✅ 写入成功：{schema}.{table} 共 {len(out)} 行")