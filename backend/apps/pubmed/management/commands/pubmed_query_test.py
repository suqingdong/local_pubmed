import time
import math
import json
from itertools import islice
from loguru import logger

from django.core.management.base import BaseCommand
from django.db import transaction, connection

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
        start_time = time.time()
        query = kwargs['query']
        embeddings = utils.get_embeddings('text-embedding-3-small')

        table = PubmedArticle._meta.db_table
        vector = embeddings.embed_query(query)

        bm25_topn = 200
        vector_topn = 200
        bm25_weight = 0.15
        top_k = 10
        start = 0
        ef_search = 40


        sql = f'''
            WITH
            -- 1️⃣ 向量主召回（HNSW，必须是第一步）
            vec_candidates AS (
                SELECT
                    pmid,
                    1 - (title_abstract_vec <=> %s::vector) AS vec_score
                FROM pubmed_articles
                ORDER BY title_abstract_vec <=> %s::vector
                LIMIT %s
            ),

            -- 2️⃣ BM25 仅做 existence filter（不排序）
            bm25_filter AS (
                SELECT pmid
                FROM pubmed_articles,
                    plainto_tsquery('english', %s) q
                WHERE ts_en @@ q
                LIMIT %s
            ),

            -- 3️⃣ 只在小集合内 merge
            hybrid AS (
                SELECT
                    v.pmid,
                    v.vec_score,
                    CASE WHEN b.pmid IS NOT NULL THEN 1.0 ELSE 0.0 END AS bm25_hit
                FROM vec_candidates v
                LEFT JOIN bm25_filter b USING (pmid)
            )

            SELECT
                pmid,
                vec_score * %s + bm25_hit * (1.0 - %s) AS hybrid_score
            FROM hybrid
            ORDER BY hybrid_score DESC
            LIMIT %s OFFSET %s;
        '''

        params = [
            vector,
            vector,
            vector_topn,

            query,
            bm25_topn,
            
            bm25_weight,
            bm25_weight,

            top_k,
            start,
        ]

        with connection.cursor() as cursor:
            cursor.execute('SET LOCAL hnsw.ef_search = %s;', [ef_search])

            # explain_sql = f"EXPLAIN (ANALYZE, BUFFERS, VERBOSE) {sql}"
            # cursor.execute(explain_sql, params)
            # plan = "\n".join(row[0] for row in cursor.fetchall())
            # print(plan)

            cursor.execute(sql, params)
            rows = cursor.fetchall()
            columns = [col[0] for col in cursor.description]

        pmid_list = [row[0] for row in rows]
        print(pmid_list)
        qs = PubmedArticle.objects.filter(pmid__in=pmid_list)
        data = PubmedArticleSerializer(qs, many=True).data
        print(json.dumps(data, indent=2))

        logger.info(f"Finished in {time.time() - start_time:.2f} seconds")
