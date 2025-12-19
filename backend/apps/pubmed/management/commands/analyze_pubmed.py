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
        parser.add_argument('--lists', help='lists', default=200, type=int)


    def handle(self, *args, **kwargs):

        option = kwargs['option']
        field = kwargs['field']
        lists = kwargs['lists']

        table = PubmedArticle._meta.db_table

        analyze_sql = f'ANALYZE {table}'

        index_sql = f'''
            CREATE INDEX CONCURRENTLY {field}_hnsw_idx
            ON {table}
            USING hnsw ({field} vector_cosine_ops)
            WITH (m = 16, ef_construction = 200);
        '''
        
        with connection.cursor() as cursor:
            if option == 'analyze':
                sql = analyze_sql
            else:
                sql = index_sql
            loguru.logger.debug(f'>>> run sql: {sql}')

            cursor.execute(sql)

        loguru.logger.info('Done')
