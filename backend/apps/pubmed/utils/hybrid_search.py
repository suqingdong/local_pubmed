from itertools import chain

import numpy as np
from django.db import transaction, connection
from django.db.models import F
from django.core.cache import cache
from django.contrib.postgres.search import SearchVector, SearchQuery, SearchRank
from pgvector.django import CosineDistance

from utils.llm import get_embeddings


def hybrid_search(query,
                  base_qs,
                  start=0,
                  top_k=10,
                  bm25_topn=200,
                  vector_topn=200,
                  bm25_weight=0.4,
                  cache_timeout=24*3600,
    ):
    """
    Hybrid search: BM25 + vector search for PubmedArticle
    使用 Django cache 缓存 embeddings
    """

    embeddings = get_embeddings()

    # --- 1. BM25 查询 ---
    rank=SearchRank(F('ts_en'), SearchQuery(query, config='english'))
    bm25_qs = base_qs.annotate(rank=rank).extra(
        where=["ts_en @@ plainto_tsquery('english', %s)"],
        params=[query]
    )
    bm25_qs = bm25_qs.filter(rank__gt=0.0).order_by('-rank')[:bm25_topn]
    # print(bm25_qs.query)
    # print(bm25_qs.explain())
    bm25_results = list(bm25_qs)

    # --- 2. 向量检索 ---
    cache_key = f"embed:{query}"
    vector = cache.get(cache_key)
    if vector is None:
        vector = embeddings.embed_query(query)
        cache.set(cache_key, tuple(vector), cache_timeout)
    
    vector_array = np.array(vector)
    vector_qs = base_qs.annotate(distance=CosineDistance('title_abstract_vector', vector_array))
    vector_qs = vector_qs.order_by('distance')[:vector_topn]
    vector_results = list(vector_qs)

    # --- 3. 合并去重 ---
    combined = {obj.pmid: obj for obj in chain(bm25_results, vector_results)}
    results = list(combined.values())

    # --- 4. 计算最终分数 ---
    for obj in results:
        if obj.title_abstract_vector is not None:
            vec = np.array(obj.title_abstract_vector)
            cosine_score = np.dot(vec, vector_array) / (np.linalg.norm(vec) * np.linalg.norm(vector_array) + 1e-8)
        else:
            cosine_score = 0.0

        bm25_score = getattr(obj, 'rank', 0.0)
        obj.hybrid_score = bm25_weight * bm25_score + (1 - bm25_weight) * cosine_score

    # --- 5. 排序并返回 top_k ---
    results = sorted(results, key=lambda x: x.hybrid_score, reverse=True)[start:start+top_k]
    return results
