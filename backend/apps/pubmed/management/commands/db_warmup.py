import time
import loguru
import random
from django.db import connection
from django.core.management.base import BaseCommand
from pubmed.models import PubmedArticle  # 确认你的模型路径


class Command(BaseCommand):
    help = 'Ultra-warmup for HNSW and GIN indexes'

    def handle(self, *args, **kwargs):
        start_time = time.time()

        with connection.cursor() as cursor:
            # --- 步骤 1: 确保 pg_prewarm 扩展已安装 ---
            cursor.execute("CREATE EXTENSION IF NOT EXISTS pg_prewarm;")

            # --- 步骤 2: 使用 pg_prewarm 物理加载索引 ---
            # 这种方式比执行查询快，因为它直接按磁盘顺序读取块
            loguru.logger.debug("📥 Loading indexes into Shared Buffers via pg_prewarm...")
            index_queries = [
                "SELECT pg_prewarm(indexrelid::regclass) FROM pg_index WHERE indexrelid::regclass::text LIKE 'idx_hnsw_part_%';",
                "SELECT pg_prewarm(indexrelid::regclass) FROM pg_index WHERE indexrelid::regclass::text LIKE 'idx_ts_en_%';"
            ]
            for sql in index_queries:
                cursor.execute(sql)
            loguru.logger.debug("✅ Physical index loading complete.")

            # --- 步骤 3: 逻辑路径激活 (HNSW Multi-Path) ---
            loguru.logger.debug("🛣️ Activating HNSW search paths with random vectors...")
            
            # 获取 5 个随机真实向量，用来打通索引的不同“入口”
            # 这样新词查询时，大概率能撞到已缓存的路径
            sample_vectors = list(PubmedArticle.objects.exclude(title_abstract_vec__isnull=True)[:5].values_list('title_abstract_vec', flat=True))
            
            for i, vec in enumerate(sample_vectors):
                # 将向量列表转为 pgvector 格式字符串
                vec_str = "[" + ",".join(map(str, vec)) + "]"
                # 调低 ef_search，模拟真实搜索压力
                cursor.execute(f"SET hnsw.ef_search = 20;")
                cursor.execute(f"SELECT pmid FROM pubmed_articles ORDER BY title_abstract_vec <=> '{vec_str}'::vector LIMIT 100;")
                loguru.logger.debug(f"   - Path {i+1}/5 activated.")

            # --- 步骤 4: GIN 索引激活 ---
            loguru.logger.debug("🚀 Final GIN index activation...")
            cursor.execute("SELECT count(*) FROM pubmed_articles WHERE ts_en @@ to_tsquery('english', 'protein');")
            
        total_time = time.time() - start_time
        loguru.logger.info(f"✨ Ultra warmup complete! Total time: {total_time:.2f}s")