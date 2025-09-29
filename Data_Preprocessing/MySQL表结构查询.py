import pymysql
import os
import re

# ======================== 配置部分 ===========================
DATABASE_NAME = "xxx"  # 修改为你的数据库名称
TABLE_NAME = "xxx"  # 修改为你的表名称

# 数据库连接信息
DB_SERVER = "localhost"
DB_PORT = 3306
DB_USER = "root"
DB_PASSWORD = "010203"


# =============================================================

def output_table_structure(database_name, table_name):
    """
    连接数据库，获取指定表的字段信息，并按照格式写入桌面上的TXT文件中。
    格式示例：
        `账户名` VARCHAR(255) COMMENT '账户名称',
    """
    # 连接数据库，使用DictCursor方便后续获取字段名称
    connection = pymysql.connect(
        host=DB_SERVER,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        database=database_name,
        charset='utf8',
        cursorclass=pymysql.cursors.DictCursor
    )

    try:
        with connection.cursor() as cursor:
            sql = f"SHOW FULL COLUMNS FROM `{table_name}`"
            cursor.execute(sql)
            columns = cursor.fetchall()
    finally:
        connection.close()

    # 获取当前用户桌面路径，并构造输出文件路径
    desktop_path = os.path.join(os.path.expanduser("~"), "Desktop")
    filename = f"{table_name}表结构.txt"
    file_path = os.path.join(desktop_path, filename)

    # 定义正则，用于解析字段类型中的长度和小数点信息
    pattern = re.compile(r"^([a-zA-Z0-9_]+)(?:\((\d+)(?:,(\d+))?\))?")

    with open(file_path, 'w', encoding='utf-8') as f:
        for col in columns:
            field_name = col["Field"]
            type_full = col["Type"]
            comment = col["Comment"]

            match = pattern.match(type_full)
            if match:
                base_type = match.group(1).upper()
                length = match.group(2)
                decimal = match.group(3)
                if length:
                    if decimal:
                        type_str = f"{base_type}({length},{decimal})"
                    else:
                        type_str = f"{base_type}({length})"
                else:
                    type_str = base_type
            else:
                type_str = type_full.upper()

            # 格式输出：`字段名` 数据类型 COMMENT '备注',
            line = f"`{field_name}` {type_str} COMMENT '{comment}',\n"
            f.write(line)

    print(f"文件已保存到: {file_path}")


if __name__ == "__main__":
    output_table_structure(DATABASE_NAME, TABLE_NAME)
