import os
import sys
from pathlib import Path
import multiprocessing

import loguru
import django

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(BASE_DIR))

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'backend.settings') # <--- 这里改成你 settings.py 所在的文件夹名
django.setup()

from django.db import connection


def build_index_for_table(table_name):
    loguru.logger.info(f"正在处理: {table_name} ...")
    try:
        # 强制获取新的连接并开启 autocommit 模式（为了执行 ALTER）
        with connection.cursor() as cursor:
           # 这里的设置只对当前子进程生效
            cursor.execute("SET maintenance_work_mem = '16GB';")
            
            # --- 核心：向量索引 (最慢) ---
            loguru.logger.debug(f"🚀 {table_name}: 构建 HNSW 向量索引...")
            cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_vec_{table_name} ON {table_name} USING hnsw (title_abstract_vec vector_cosine_ops) WITH (m = 8, ef_construction = 64);")
            
            # --- 重点：PMID 索引 (身份证) ---
            loguru.logger.debug(f"🆔 {table_name}: 构建 PMID 索引...")
            cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_pmid_{table_name} ON {table_name} (pmid);")
            
            # --- 重点：Factor 索引 (排序位) ---
            loguru.logger.debug(f"📊 {table_name}: 构建 factor 索引...")
            cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_factor_{table_name} ON {table_name} (factor);")

            # --- 全文索引 (关键词) ---
            loguru.logger.debug(f"📝 {table_name}: 构建 GIN 全文索引...")
            cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_gin_{table_name} ON {table_name} USING GIN (ts_en);")
            
            cursor.execute(f"ANALYZE {table_name};")
        loguru.logger.info(f"✅ {table_name} 竣工！")
    except Exception as e:
        loguru.logger.error(f"❌ {table_name} 失败: {e}")


def main():
    # 获取任务列表
    with connection.cursor() as cursor:
        cursor.execute("""
            SELECT tablename 
            FROM pg_catalog.pg_tables 
            WHERE schemaname = 'public' 
            AND tablename ~ '^p_.*_h[0-9]+$'
        """)
        tables = [row[0] for row in cursor.fetchall()]

    # 直接跑失败的几个
    tables = ['p_history_h4']

    loguru.logger.info(f"检测到 {len(tables)} 个分区。采用 2 路并发，每路分配 24 核。")

    # 并发数设为 2。 2路 * 24核 = 48核，给系统留点余量。
    # 这样磁盘 IO 压力更均衡，不会产生严重的 Wait Event
    with multiprocessing.Pool(processes=2) as pool:
        pool.map(build_index_for_table, tables)
    
    loguru.logger.info("🎉 所有任务已完成！")


if __name__ == "__main__":
    main()