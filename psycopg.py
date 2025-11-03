import psycopg2
from psycopg2 import OperationalError

def test_connection():
    try:
        # 替换为你的实际参数
        connection = psycopg2.connect(
            host="pgm-uf6q0m0aa2zqx3z4co.pg.rds.aliyuncs.com",
            port=5432,
            user="kering_admin",
            password="#cow8TUEnbQ",  
            database="content_tagging"  
        )
        print("✅ 连接成功！")
        connection.close()
    except OperationalError as e:
        print(f"❌ 连接失败: {e}")

if __name__ == "__main__":
    test_connection()