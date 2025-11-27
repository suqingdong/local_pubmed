import re
import sys
import json
import time
from pathlib import Path

import loguru

from django.core.management.base import BaseCommand
from django.db import transaction, connection

from pubmed.models import PubmedArticle
import utils


def load_json_data(json_file):
    with open(json_file) as f:
        for line in f:
            data = json.loads(line)
            data['title'] = re.sub(r'\s+', ' ', data['title'].strip())
            data['abstract'] = re.sub(r'\s+', ' ', data['abstract'].strip())
            yield data


def get_bulk_articles(json_file, batch_size):
    bulk_articles = []
    for data in load_json_data(json_file):
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
    for json_file in data_path:
        with transaction.atomic():
            count = 0
            for bulk_articles in get_bulk_articles(json_file, batch_size):
                PubmedArticle.objects.bulk_create(bulk_articles)
                count += len(bulk_articles)
                sys.stderr.write(f'\r>>> {count} articles loaded')
                sys.stderr.flush()
            sys.stderr.write('\n')




def create_article(data_path, mode):
    """逐条插入数据，速度慢，适合追踪异常数据
    """
    for json_file in data_path:
        with transaction.atomic():
            for n, data in enumerate(load_json_data(json_file), 1):
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
        parser.add_argument('-b', '--batch-size', help='Batch size for bulk create', type=int, default=10000)
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