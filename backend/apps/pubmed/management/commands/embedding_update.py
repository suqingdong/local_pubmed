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
    # 1. 创建内存中的文本流 (CSV 格式)
    # 格式：pmid|vector_string
    f = io.StringIO()
    for row in batch_data:
        # 将 [0.1, 0.2, ...] 转换为 '[0.1,0.2,...]'
        vec_str = '[' + ','.join(map(str, row['vec'])) + ']'
        f.write(f"{row['pmid']}\t{vec_str}\n")
    f.seek(0)

    # 2. 创建临时表 (不记日志，极快)
    cursor.execute(f"CREATE TEMP TABLE tmp_vectors (pmid INT, vec vector) ON COMMIT DROP;")

    # 3. 使用 COPY 快速导入临时表
    cursor.copy_from(f, 'tmp_vectors', columns=('pmid', 'vec'))

    # 4. 利用数据库内部 Join 完成更新 (这步是毫秒级的)
    cursor.execute(f"""
        UPDATE {table} AS t
        SET title_abstract_vec = v.vec
        FROM tmp_vectors v
        WHERE t.pmid = v.pmid;
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
            objs = [PubmedArticle(pmid=row['pmid'], title_abstract_vec=row['vec']) for row in batch_data]
            with transaction.atomic():

                # SET synchronous_commit = off; -- 牺牲一点安全性换取极速写入
                with connection.cursor() as cursor:
                    cursor.execute('SET synchronous_commit = off;')

                    # copy_update_vectors(cursor, table, batch_data)

                    PubmedArticle.objects.bulk_update(
                        objs,
                        ['title_abstract_vec'],
                        batch_size=2000,
                    )

            complete_count += len(batch_data)
            logger.debug(f'Processed {complete_count} articles')

        logger.info(f"Finished in {time.time() - start_time:.2f} seconds")
