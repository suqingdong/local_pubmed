import time

import numpy as np
import loguru

from django.core.management.base import BaseCommand

from django.db import transaction, connection
from django.db.models import F
from django.core.cache import cache
from django.contrib.postgres.search import SearchVector, SearchQuery, SearchRank
from pgvector.django import CosineDistance

import utils
from pubmed.models import PubmedArticle
from pubmed.serializers import PubmedArticleSerializer




def hybrid_search(query,
                  base_qs,
                  start=0,
                  top_k=10,
                  bm25_topn=200,
                  vector_topn=200,
                  cache_timeout=24*3600,
    ):
    """
    Hybrid search: BM25 + vector search for PubmedArticle
    使用 Django cache 缓存 embeddings
    """

    # RRF 算法的常数，通常取 60
    K = 60

    embeddings = utils.get_embeddings('text-embedding-3-small')
    vector = embeddings.embed_query(query)
    vector_array = np.array(vector)

    # --- 1：BM25 召回 (仅取 ID 和 排名) ---
    start_t = time.time()
    weights = [0.1, 0.2, 0.4, 1.0]
    rank = SearchRank(F('ts_en'), SearchQuery(query, config='english'), weights=weights)
    bm25_qs = base_qs.annotate(rank=rank).extra(
        where=["ts_en @@ plainto_tsquery('english', %s)"],
        params=[query]
    )
    bm25_qs = bm25_qs.filter(rank__gt=0.0).order_by('-rank')[:bm25_topn]
    bm25_list = list(bm25_qs.values_list('pmid', flat=True))
    print(f"BM25 Done: {time.time() - start_t:.2f}s")


    # --- 2：向量召回 (仅取 ID 和 排名) ---
    start_v = time.time()
    vector_qs = (
        base_qs.annotate(
            distance=CosineDistance('title_abstract_vec', vector_array)
        )
        # .filter(distance__lt=0.6)
        .order_by('distance')
        .only('pmid')[:vector_topn]
    )

    vector_list = list(vector_qs.values_list('pmid', flat=True))
    print(f"Vector Done: {time.time() - start_v:.2f}s")

    # # print(bm25_qs.explain())
    # # print(vector_qs.explain())

    # # --- 3. RRF 融合 (Reciprocal Rank Fusion) ---
    # # rrf_score = sum( 1 / (rank + K) )
    # rrf_scores = {}

    # weight_bm25 = 1.0
    # weight_vector = 0.7  # 降低向量检索的权重

    # # 处理 BM25 排名
    # for rank, obj in enumerate(bm25_list, start=1):
    #     rrf_scores[obj.pmid] = rrf_scores.get(obj.pmid, 0) + weight_bm25 / (K + rank)

    # # 处理向量排名
    # for rank, obj in enumerate(vector_list, start=1):
    #     rrf_scores[obj.pmid] = rrf_scores.get(obj.pmid, 0) + weight_vector / (K + rank)

    # # 按 RRF 分数从高到低排序，取最终 top_k 个 PMID
    # final_pmids = sorted(rrf_scores.keys(), key=lambda x: rrf_scores[x], reverse=True)[start:start+top_k]




class Command(BaseCommand):
    help = 'Query hybrid search implementation test'

    def add_arguments(self, parser):
        parser.add_argument('query', help='query string')
        parser.add_argument('--year', help='filter by year', type=int, default=2020)
        parser.add_argument('--factor', help='filter by factor', type=float, default=5.0)

    def handle(self, *args, **kwargs):
        query = kwargs['query']
        year = kwargs['year']
        factor = kwargs['factor']
        
        # 向量检索
        # embeddings = utils.get_embeddings('text-embedding-3-small')
        # vector = embeddings.embed_query(query)
        # vector_array = np.array(vector)

        start_time = time.time()

        base_qs = PubmedArticle.objects.filter(
            year__gte=year,
            factor__gte=factor,
        )

        with connection.cursor() as cursor:
            # 强制减小搜索节点数量，这能显著降低 NAS 的 IO 压力
            cursor.execute('SET LOCAL hnsw.ef_search = 80;')
            cursor.execute('SET LOCAL work_mem = "256MB" ')
            cursor.execute('SET LOCAL enable_indexscan = on;')

            # 强制开启并行
            cursor.execute('SET LOCAL max_parallel_workers_per_gather = 4;') 
            cursor.execute('SET LOCAL max_parallel_maintenance_workers = 4;')
            cursor.execute('SET LOCAL min_parallel_table_scan_size = 0;')
            cursor.execute('SET LOCAL parallel_setup_cost = 0;')

            hybrid_search(query, base_qs=base_qs, top_k=10)

            # start_db = time.time()
            # # 用原生 SQL 跑同样逻辑，只取 ID，不实例化对象
            # cursor.execute("""
            #     SELECT pmid FROM pubmed_articles 
            #     WHERE year >= %s
            #     ORDER BY title_abstract_vec <=> %s::vector 
            #     LIMIT 100
            # """, [year, vector_array.tolist()])
            # rows = cursor.fetchall()
            # db_elapsed = time.time() - start_db
            # loguru.logger.debug(f"🗄️ Pure DB time: {db_elapsed:.2f}s")

            # qs = PubmedArticle.objects.all()

            # qs = PubmedArticle.objects.filter(
            #     year__gte=year,
            #     factor__gte=factor,
            # )
            # vector_qs = (
            #     qs.annotate(
            #         distance=CosineDistance('title_abstract_vec', vector_array)
            #     )
            #     # .filter(distance__lt=0.6)
            #     .order_by('distance')
            #     .only('pmid')[:100]
            # )
            # print(vector_qs.explain())
            # results = list(vector_qs)

        loguru.logger.debug(f"✨ Query time: {time.time() - start_time:.2f}s")
