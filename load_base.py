# /root/ysl_project/load_base.py
import os
import pandas as pd
from utils import write_to_postgres, load_config

CONFIG_PATH = "/root/ysl_project/ysl_config.json"  # 你的新配置

combined_df = pd.read_csv("/root/ysl_project/df")

# 可选：为大表设置合适的字符串类型（减少类型推断的开销）
for col in combined_df.select_dtypes(include='object').columns:
    combined_df[col] = combined_df[col].astype("string")

# 写入到 public.base_posts_v1，整表覆盖
write_to_postgres(
    df=combined_df,
    table_name="public.base_posts_v1",
    mode="overwrite",
    batch_size=20000  # 批大小，按网络/库性能可调
)

print("✅ Done: wrote to public.base_posts_v1")