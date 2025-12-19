from itertools import chain

import numpy as np
from django.db import transaction, connection
from django.db.models import F
from django.core.cache import cache
from django.contrib.postgres.search import SearchVector, SearchQuery, SearchRank
from pgvector.django import CosineDistance

import utils


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

    cache_key = f"embed:{query}"
    vector = cache.get(cache_key)
    if vector is None or len(vector) != 1536:
        vector = embeddings.embed_query(query)
        cache.set(cache_key, tuple(vector), cache_timeout)
    vector_array = np.array(vector)

    # --- 1：BM25 召回 (仅取 ID 和 排名) ---
    rank=SearchRank(F('ts_en'), SearchQuery(query, config='english'))
    bm25_qs = base_qs.annotate(rank=rank).extra(
        where=["ts_en @@ plainto_tsquery('english', %s)"],
        params=[query]
    )
    bm25_qs = bm25_qs.filter(rank__gt=0.0).order_by('-rank')[:bm25_topn]

    # --- 2：向量召回 (仅取 ID 和 排名) ---
    vector_qs = (
        base_qs.annotate(
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
    final_objs = base_qs.filter(pmid__in=final_pmids).only(
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

    return results
