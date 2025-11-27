import csv
import sys
import json
import time
from pathlib import Path

import loguru

from django.core.management.base import BaseCommand
from django.db import transaction, connection

from pubmed.models import PubmedArticle
import utils


def load_xml_to_csv(xml, csv_file=None):
    csv_file = csv_file or xml + '.csv'
    if Path(csv_file).exists():
        loguru.logger.warning(f'File {csv_file} already exists. Skipping.')
        return csv_file
    
    fields = PubmedArticle._meta.fields
    all_fields = [field.name for field in fields]
    json_fields = [field.name for field in fields if field.get_internal_type() == 'JSONField']
    with open(csv_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(all_fields)

        for data in utils.load_pubmed_xml(xml):
            row_values = [
                json.dumps(data.get(field)) if field in json_fields else data.get(field)
                for field in all_fields
            ]
            writer.writerow(row_values)
    return csv_file


def copy_from_csv(csv_file):
    table = PubmedArticle._meta.db_table

    with connection.cursor() as cursor:
        with open(csv_file, 'r', encoding='utf-8') as f:
            sql = f'COPY {table} FROM STDIN WITH CSV HEADER'
            cursor.copy_expert(sql, f)
        # cursor.execute(f'ANALYZE {table}')



def get_bulk_articles(xml, batch_size):
    bulk_articles = []
    for data in utils.load_pubmed_xml(xml):
        article = PubmedArticle(**data)
        bulk_articles.append(article)
        if len(bulk_articles) == batch_size:
            yield bulk_articles
            bulk_articles = []
    if bulk_articles:
        yield bulk_articles


# 批量导入数据
def bulk_create_articles(data_path, batch_size):
    """批量插入数据，速度快，但内存占用大
    """
    total = len(data_path)
    for file_num, xml in enumerate(data_path, 1):
        loguru.logger.debug(f'>>> loading {xml} [{file_num}/{total}]')

        with transaction.atomic():
            count = 0
            for bulk_articles in get_bulk_articles(xml, batch_size):
                PubmedArticle.objects.bulk_create(bulk_articles)
                count += len(bulk_articles)
                sys.stderr.write(f'\r>>> {count} articles loaded')
                sys.stderr.flush()
            sys.stderr.write('\n')


# def bulk_create_articles(data_path, batch_size):
#     """批量插入数据，速度快，但内存占用大
#     """
#     total = len(data_path)
#     for file_num, xml in enumerate(data_path, 1):
#         loguru.logger.debug(f'>>> loading {xml} [{file_num}/{total}]')

#         try:
#             csv_file = load_xml_to_csv(xml)
#             loguru.logger.debug(f'>>> copy from csv file: {csv_file}')
#             copy_from_csv(csv_file)
#         except Exception as e:
#             loguru.logger.error(f'Error importing {xml}: {e}')
#             create_article([xml], 'update')


def create_article(data_path, mode):
    """逐条插入数据，速度慢，适合追踪异常数据
    """
    total = len(data_path)
    for file_num, xml in enumerate(data_path, 1):
        loguru.logger.debug(f'>>> loading {xml} [{file_num}/{total}]')
        result = utils.load_pubmed_xml(xml)
        with transaction.atomic():
            for n, data in enumerate(result, 1):
                pmid = data['pmid']
                try:
                    if mode == 'insert':
                        PubmedArticle.objects.create(**data)
                    elif mode == 'update':
                        PubmedArticle.objects.update_or_create(pmid=pmid, defaults=data)
                    if n % 1000 == 0:
                        sys.stderr.write(f'\r>>> {n} articles loaded')
                        sys.stderr.flush()
                except Exception as e:
                    loguru.logger.error(f'Error updating article {pmid}: {e}')
                    print(data)
                    exit(1)


class Command(BaseCommand):
    help = 'Initialize PubMed database'

    def add_arguments(self, parser):
        parser.add_argument('data_path', type=str, help='Path to the PubMed data', nargs='*')
        parser.add_argument('-d', '--drop', action='store_true', help='Drop existing data before loading')
        parser.add_argument('-b', '--batch-size', help='Batch size for bulk create', type=int, default=1000)
        parser.add_argument('-m', '--mode', help='mode of create', choices=['insert', 'update'], default='insert')

    def handle(self, *args, **kwargs):
        data_path = kwargs['data_path']
        batch_size = kwargs['batch_size']
        mode = kwargs['mode']

        start_time = time.time()

        if kwargs['drop']:
            PubmedArticle.objects.all().delete()
            loguru.logger.debug('deleted all existing PubmedArticle data')

        if batch_size > 1:
            try:
                bulk_create_articles(data_path, batch_size)
            except Exception as e:
                loguru.logger.warning(f'Error importing data: {e}')
                create_article(data_path, 'update')
        else:
            create_article(data_path, mode)

        loguru.logger.debug(f'time elapsed: {time.time() - start_time:.2f} seconds')