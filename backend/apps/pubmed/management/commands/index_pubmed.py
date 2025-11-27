from pathlib import Path

import loguru
from django.core.management.base import BaseCommand
from django.db import connection

from pubmed.models import PubmedArticle


all_index_fields = [
    'title',
    'abstract',
    'year',
    'pubmed_pubdate',
    'authors',
    'affiliations',
    'impact_factor',
    'pub_types',
]


def list_indexes(table):
    with connection.cursor() as cursor:
        cursor.execute(f"SELECT indexname FROM pg_indexes WHERE tablename = '{table}'")
        for row in cursor.fetchall():
            print(row[0])


class Command(BaseCommand):
    help = 'Initialize PubMed database'

    def add_arguments(self, parser):
        parser.add_argument('fields', type=str, help='Fields to index', nargs='*')
        parser.add_argument('-o', '--operation', type=str, help='Operation to perform', default='add', choices=['add', 'remove', 'list'])


    def handle(self, *args, **kwargs):
        operation = kwargs['operation']
        fields = kwargs['fields']

        table = PubmedArticle._meta.db_table

        if operation == 'list':
            list_indexes(table)
            return

        if fields == ['all'] or not fields:
            fields = all_index_fields

        available_fields = [f.name for f in PubmedArticle._meta.fields]
        
        with connection.cursor() as cursor:
            for field in fields:
                if field not in available_fields:
                    loguru.logger.warning(f'Field {field} not available')
                    continue

                index_name = f'{table}_{field}_idx'
                if operation == 'add':
                    sql = f'CREATE INDEX IF NOT EXISTS {index_name} ON {table} ({field})'
                elif operation == 'remove':
                    sql = f'DROP INDEX IF EXISTS {index_name}'
                cursor.execute(sql)

                loguru.logger.info(f'{operation} index `{index_name}` on `{table}.{field}`')
        loguru.logger.info('Done')
