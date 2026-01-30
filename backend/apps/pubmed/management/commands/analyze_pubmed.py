from pathlib import Path

import loguru
from django.core.management.base import BaseCommand
from django.db import connection

from pubmed.models import PubmedArticle


class Command(BaseCommand):
    help = 'ANALYZE/INDEX PubMed database'

    def add_arguments(self, parser):
        parser.add_argument('-o', '--option', help='analyze or index', default='analyze', choices=['analyze', 'index'])
        parser.add_argument('--field', help='field name', default='title_abstract_vec')
        parser.add_argument('--full', action='store_true', help='full analyze')


    def handle(self, *args, **kwargs):

        option = kwargs['option']
        field = kwargs['field']
        full = kwargs['full']

        table = PubmedArticle._meta.db_table

        if full:
            analyze_sql = f'VACUUM (FULL, ANALYZE, VERBOSE) {table}'  # FULL 更彻底, 会锁表
        else:
            analyze_sql = f'VACUUM (ANALYZE, VERBOSE) {table}'        # 快速版, 不锁表

        concurrently = 'CONCURRENTLY'  # 创建索引时不锁表
        concurrently = ''              # 创建索引时锁表，速度更快

        index_sql = f'''
            -- 强制更新统计信息，让优化器知道有 2000 万行
            -- ANALYZE {table};

            -- 内存配置
            SET maintenance_work_mem = '128GB';
        
            CREATE INDEX {concurrently} {field}_hnsw_idx
            ON {table}
            USING hnsw ({field} vector_cosine_ops)
            WITH (m = 12, ef_construction = 64);
        '''

        with connection.cursor() as cursor:
            if option == 'analyze':
                sql = analyze_sql
            else:
                sql = index_sql
            loguru.logger.debug(f'>>> run sql: {sql}')

            cursor.execute(sql)

        loguru.logger.info('Done')
