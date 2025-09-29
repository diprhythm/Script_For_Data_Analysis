import pymysql

MYSQL_HOST = "localhost"
MYSQL_USER = "root"
MYSQL_PASSWORD = "010203"

MYSQL_TUNING_QUERIES = [
    # CPU 优化
    "SET GLOBAL innodb_thread_concurrency = 0;",
    "SET GLOBAL thread_cache_size = 1000;",
    "SET GLOBAL performance_schema = 0;",

    # 内存优化
    "SET GLOBAL innodb_buffer_pool_size = 96 * 1024 * 1024 * 1024;",  # 96G
    "SET GLOBAL innodb_buffer_pool_instances = 16;",
    "SET GLOBAL innodb_log_buffer_size = 1024 * 1024 * 1024;",  # 1G
    "SET GLOBAL max_connections = 10000;",
    "SET GLOBAL max_user_connections = 2000;",

    # 磁盘 I/O 优化
    "SET GLOBAL innodb_flush_log_at_trx_commit = 2;",
    "SET GLOBAL innodb_io_capacity = 16000;",
    "SET GLOBAL innodb_io_capacity_max = 32000;",
    "SET GLOBAL sync_binlog = 0;",

    # 网络优化
    "SET GLOBAL skip_name_resolve = 1;",
    "SET GLOBAL net_buffer_length = 16 * 1024 * 1024;",  # 16M
    "SET GLOBAL max_allowed_packet = 256 * 1024 * 1024;",  # 256M

    # 表缓存优化
    "SET GLOBAL table_open_cache = 100000;",
    "SET GLOBAL table_definition_cache = 5000;"
]

def optimize_mysql():
    connection = None
    try:
        connection = pymysql.connect(
            host=MYSQL_HOST,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            cursorclass=pymysql.cursors.DictCursor
        )

        with connection.cursor() as cursor:
            for query in MYSQL_TUNING_QUERIES:
                try:
                    cursor.execute(query)
                    print(f"✅ 成功执行: {query}")
                except pymysql.MySQLError as e:
                    print(f"⚠️ 忽略错误: {query} - {e}")

        connection.commit()
        print("\n🚀 MySQL 配置优化完成！")

    except pymysql.MySQLError as e:
        print(f"❌ 发生错误: {e}")

    finally:
        if connection and connection.open:
            connection.close()
            print("🔌 MySQL 连接已关闭")

if __name__ == "__main__":
    optimize_mysql()
