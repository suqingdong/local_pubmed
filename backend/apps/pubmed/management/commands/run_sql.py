from pathlib import Path
from loguru import logger

from django.core.management.base import BaseCommand
from django.db import transaction, connection

from pubmed.models import PubmedArticle
from pubmed.serializers import PubmedArticleSerializer


class Command(BaseCommand):
    help = 'Run arbitrary SQL command'

    def add_arguments(self, parser):
        parser.add_argument('sql', help='SQL string/file')

    def handle(self, *args, **kwargs):
        sql = kwargs['sql']

        try:
            if Path(sql).is_file():
                sql = Path(sql).read_text()
        except Exception:
            pass
        
        logger.debug(f'>>> run sql: {sql}')
        with connection.cursor() as cursor:
            cursor.execute(sql)
            if cursor.description:  # 只有当有结果集描述时，才去 fetch
                result = cursor.fetchall()
            else:
                result = "Command executed successfully (no rows returned)."
            print(result)