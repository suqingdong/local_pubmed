import re
import sys
import json
import time
import datetime as dt
from pathlib import Path

import loguru

from django.core.management.base import BaseCommand
from django.db import transaction, connection

from pubmed.models import PubmedArticle



def _parse_date(v):
    """把 'YYYY-MM-DD' / date / None 转成 date"""
    if v is None or v == "":
        return None
    if isinstance(v, dt.date):
        return v
    if isinstance(v, str):
        # 只处理你示例这种最常见格式；需要更复杂格式再扩展
        return dt.date.fromisoformat(v)
    return v


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
        if hasattr(article, 'ts_en'):
            del article.ts_en

        bulk_articles.append(article)
        if len(bulk_articles) == batch_size:
            yield bulk_articles
            bulk_articles = []
    if bulk_articles:
        yield bulk_articles



def _raw_insert_pubmed_articles(objs, ignore_conflicts=True, page_size=2000):
    if not objs:
        return 0

    model = objs[0].__class__
    table = model._meta.db_table
    qn = connection.ops.quote_name

    # 排除 generated column
    fields = [
        f for f in model._meta.concrete_fields
        if f.column is not None and f.name != "ts_en"
    ]

    cols_sql = ", ".join(qn(f.column) for f in fields)
    pk_col = qn(model._meta.pk.column)

    try:
        from psycopg2.extras import execute_values, Json
    except Exception as e:
        raise RuntimeError("需要 psycopg2/psycopg2-binary 才能运行该 raw insert。") from e

    def adapt_value(field, value):
        if value is None:
            return None

        # 更稳：用内部类型判断 JSONField（兼容不同 JSONField 实现）
        internal = getattr(field, "get_internal_type", lambda: "")()
        if internal == "JSONField":
            return Json(value)

        # 或者按 db_type 判断（jsonb / json）
        dbt = (field.db_type(connection) or "").lower()
        if "jsonb" in dbt or dbt == "json":
            return Json(value)

        # 日期字段：把字符串转 date，更稳
        if internal == "DateField":
            return _parse_date(value)

        return value

    rows = []
    for obj in objs:
        row = []
        for f in fields:
            v = getattr(obj, f.attname)
            row.append(adapt_value(f, v))
        rows.append(row)

    sql = f"INSERT INTO {qn(table)} ({cols_sql}) VALUES %s"
    if ignore_conflicts:
        sql += f" ON CONFLICT ({pk_col}) DO NOTHING"

    with connection.cursor() as cursor:
        execute_values(cursor, sql, rows, page_size=2000)

    return len(objs)


# 批量导入数据
def bulk_create_articles(data_path, batch_size, ignore_conflicts=True):
    """批量插入数据，速度快，但内存占用大
    """
    count = 0
    for json_file in data_path:
        for bulk_articles in get_bulk_articles(json_file, batch_size):
            with transaction.atomic():
                # PubmedArticle.objects.bulk_create(
                #     bulk_articles,
                #     ignore_conflicts=ignore_conflicts,
                # )
                # ✅ 用 raw insert 替代 bulk_create
                _raw_insert_pubmed_articles(
                    bulk_articles,
                    ignore_conflicts=ignore_conflicts,
                    page_size=batch_size,   # 你也可以固定成 2000/5000
                )
                count += len(bulk_articles)
                loguru.logger.debug(f'inserted {count} articles')


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