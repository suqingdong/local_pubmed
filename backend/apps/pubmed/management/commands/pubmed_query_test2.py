import time
import math
import json
from itertools import islice
from loguru import logger

from django.core.management.base import BaseCommand
from django.db import transaction, connection
from itertools import chain

import numpy as np
from django.db.models import F
from django.core.cache import cache
from django.contrib.postgres.search import SearchVector, SearchQuery, SearchRank
from pgvector.django import CosineDistance

from pubmed.models import PubmedArticle
from pubmed.serializers import PubmedArticleSerializer
import utils


def read_jsonl_batches(path, batch_size):
    with utils.safe_open(path, 'rb') as f:
        while True:
            lines = list(islice(f, batch_size))
            if not lines:
                break
            yield [json.loads(l) for l in lines]


class Command(BaseCommand):
    help = 'Embedding PubMed Database'

    def add_arguments(self, parser):
        parser.add_argument('-q', '--query', help='Query string', required=True)

    def handle(self, *args, **kwargs):
        query = kwargs['query']
        embeddings = utils.get_embeddings('text-embedding-3-small')

        vector = embeddings.embed_query(query)
        vector_array = np.array(vector)

        bm25_topn = 200
        vector_topn = 200
        bm25_weight = 0.4
        top_k = 10
        start = 0
        ef_search = 64

        # RRF 算法的常数，通常取 60
        K = 60

        start_time = time.time()
        with transaction.atomic():
            with connection.cursor() as cursor:
                cursor.execute('SET LOCAL hnsw.ef_search=%s;', [ef_search])
                cursor.execute('SET LOCAL work_mem = "256MB";') # 甚至更高，取决于你的内存大小

                base_qs = PubmedArticle.objects.all()

                # --- 1：BM25 召回 (仅取 ID 和 排名) ---
                rank=SearchRank(F('ts_en'), SearchQuery(query, config='english'))
                bm25_qs = base_qs.annotate(rank=rank).extra(
                    where=["ts_en @@ plainto_tsquery('english', %s)"],
                    params=[query]
                )
                bm25_qs = bm25_qs.filter(rank__gt=0.0).order_by('-rank')[:bm25_topn]
            
                # --- 2：向量召回 (仅取 ID 和 排名) ---
                vector_qs = (
                    PubmedArticle.objects.annotate(
                        distance=CosineDistance('title_abstract_vec', vector_array)
                    )
                    .order_by('distance')
                    .only('pmid')[:vector_topn]
                )

                # 触发查询并转换为列表
                bm25_list = list(bm25_qs)
                vector_list = list(vector_qs)
                # print(bm25_qs.explain())
                # print(vector_qs.explain())

                # --- 3. RRF 融合 (Reciprocal Rank Fusion) ---
                # rrf_score = sum( 1 / (rank + K) )
                rrf_scores = {}

                # 处理 BM25 排名
                for rank, obj in enumerate(bm25_list, start=1):
                    rrf_scores[obj.pmid] = rrf_scores.get(obj.pmid, 0) + 1.0 / (K + rank)

                # 处理向量排名
                for rank, obj in enumerate(vector_list, start=1):
                    rrf_scores[obj.pmid] = rrf_scores.get(obj.pmid, 0) + 1.0 / (K + rank)

                # 按 RRF 分数从高到低排序，取最终 top_k 个 PMID
                final_pmids = sorted(rrf_scores.keys(), key=lambda x: rrf_scores[x], reverse=True)[start:start+top_k]

                # --- 4. 批量回表取完整字段 (Hydration) ---
                # 只取前端展示需要的字段，避免 select *
                # 如果你需要返回向量字段进行进一步计算，可以在 .only() 中加上 'title_abstract_vec'
                final_objs = PubmedArticle.objects.filter(pmid__in=final_pmids).only(
                    'pmid',
                    'title',
                    'abstract',
                    'year',
                    'pubmed_pubdate',
                    'factor',
                    'jcr',
                    'journal',
                    'pagination',
                    'volume',
                    'authors',
                    'doi',
                    'pmc',
                )

                # 注意：filter(pmid__in=...) 会破坏 RRF 的排序顺序，需要手动恢复顺序
                obj_map = {obj.pmid: obj for obj in final_objs}
                results = [obj_map[pid] for pid in final_pmids if pid in obj_map]

                print(results[0])

        logger.info(f"Finished in {time.time() - start_time:.2f} seconds")
