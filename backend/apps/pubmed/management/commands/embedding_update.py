import io
import time
import json
from itertools import islice
from loguru import logger

from django.core.management.base import BaseCommand
from django.db import transaction, connection
from psycopg2.extras import execute_values

from pubmed.models import PubmedArticle
import utils


def read_jsonl_batches(input_files, batch_size):
    for path in input_files:
        logger.debug(f'>>> Reading file: {path}')
        with utils.safe_open(path, 'rb') as f:
            while True:
                lines = list(islice(f, batch_size))
                if not lines:
                    break
                yield [json.loads(l) for l in lines]



def copy_update_vectors(cursor, table, batch_data):
    logger.debug(f"Copying and updating {len(batch_data)} vectors into {table}...")
    # 1. 准备 CSV 数据，增加 year 字段
    f = io.StringIO()
    for row in batch_data:
        vec_str = '[' + ','.join(map(str, row['vec'])) + ']'
        # 注意：你的 batch_data 里必须包含 year！
        f.write(f"{row['pmid']}\t{row['year']}\t{vec_str}\n")
    f.seek(0)

    # 2. 创建临时表（增加 year 以利用分区裁剪）
    cursor.execute("CREATE TEMP TABLE tmp_vectors (pmid INT, year INT, vec vector) ON COMMIT DROP;")

    # 3. COPY 导入
    cursor.copy_from(f, 'tmp_vectors', columns=('pmid', 'year', 'vec'), sep='\t')

    # 4. 重点！利用 year 进行 Join 更新
    # 这样 Postgres 只会去改 p_future 或 p_2021_2025 的特定子表
    cursor.execute(f"""
        UPDATE {table} AS t
        SET title_abstract_vec = v.vec
        FROM tmp_vectors v
        WHERE t.pmid = v.pmid AND t.year = v.year;
    """)


class Command(BaseCommand):
    help = 'Embedding PubMed Database'

    def add_arguments(self, parser):
        parser.add_argument('-i', '--input-files', help='Input file', required=True, nargs='*')
        parser.add_argument('-b', '--batch-size', help='Batch size for bulk create', type=int, default=2000)

    def handle(self, *args, **kwargs):
        input_files = kwargs['input_files']
        batch_size = kwargs['batch_size']

        start_time = time.time()

        table = PubmedArticle._meta.db_table

        complete_count = 0
        for batch_data in read_jsonl_batches(input_files, batch_size):
            # objs = [
            #     PubmedArticle(
            #         pmid=row['pmid'],
            #         year=row['year'],
            #         title_abstract_vec=row['vec'],
            #     ) for row in batch_data
            # ]
            with transaction.atomic():

                # SET synchronous_commit = off; -- 牺牲一点安全性换取极速写入
                with connection.cursor() as cursor:
                    cursor.execute('SET LOCAL synchronous_commit = off;')
                    cursor.execute("SET LOCAL maintenance_work_mem = '1GB';")

                    copy_update_vectors(cursor, table, batch_data)

                    # PubmedArticle.objects.bulk_update(
                    #     objs,
                    #     ['title_abstract_vec'],
                    #     batch_size=2000,
                    # )

            complete_count += len(batch_data)
            logger.debug(f'Processed {complete_count} articles')

        logger.info(f"Finished in {time.time() - start_time:.2f} seconds")
