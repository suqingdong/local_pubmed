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



def _raw_insert_pubmed_articles(objs, ignore_conflicts=False, page_size=2000):
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
        # 1. 临时关闭同步提交，追求极致导入速度
        # LOCAL 表示只对当前这个数据库连接生效，执行完后会自动恢复
        cursor.execute("SET LOCAL synchronous_commit = off;")
        cursor.execute("SET LOCAL maintenance_work_mem = '1GB';")
        execute_values(cursor, sql, rows, page_size=page_size)

    return len(objs)


# 批量导入数据
def bulk_create_articles(data_path, batch_size, ignore_conflicts=False):
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
    """逐条插入数据，使用 Raw SQL 彻底绕过生成列报错"""
    # 提前准备好字段名和占位符（排除 ts_en）
    model = PubmedArticle
    fields = [f for f in model._meta.concrete_fields if f.name != 'ts_en']
    columns = [connection.ops.quote_name(f.column) for f in fields]
    placeholders = ["%s"] * len(fields)
    
    # 构建基础插入语句
    insert_sql = f"INSERT INTO {connection.ops.quote_name(model._meta.db_table)} ({', '.join(columns)}) VALUES ({', '.join(placeholders)})"
    
    # 如果需要更新模式，追加冲突处理
    if mode == 'update':
        # 假设你的冲突键是 pmid 和 year
        update_cols = [f"{col} = EXCLUDED.{col}" for col in columns if col not in ['"pmid"', '"year"']]
        insert_sql += f" ON CONFLICT (pmid, year) DO UPDATE SET {', '.join(update_cols)}"
    else:
        insert_sql += " ON CONFLICT (pmid, year) DO NOTHING"

    for json_file in data_path:
        with connection.cursor() as cursor:
            cursor.execute("SET LOCAL maintenance_work_mem = '1GB';")
            cursor.execute("SET LOCAL synchronous_commit = off;")
            for n, data in enumerate(load_json_data(json_file), 1):
                data.pop('ts_en', None)
                
                # 🔥 关键修正：手动处理 JSON 字段的适配
                vals = []
                for f in fields:
                    val = data.get(f.attname)
                    # 检查字段是否为 JSONField 或内容为 list/dict
                    internal_type = f.get_internal_type()
                    if (internal_type == "JSONField" or isinstance(val, (list, dict))) and val is not None:
                        # 转换成 JSON 字符串，psycopg2 会自动处理 jsonb 适配
                        from psycopg2.extras import Json
                        vals.append(Json(val))
                    else:
                        vals.append(val)
                
                try:
                    cursor.execute(insert_sql, vals)
                    if n % 1000 == 0:
                        sys.stderr.write(f'\r>>> {n} articles processed')
                        sys.stderr.flush()
                except Exception as e:
                    loguru.logger.error(f"Error at PMID {data.get('pmid')}: {e}")
                    exit(1)


class Command(BaseCommand):
    help = 'Initialize PubMed database'

    def add_arguments(self, parser):
        parser.add_argument('data_path', type=str, help='Path to the PubMed data', nargs='*')
        parser.add_argument('-d', '--drop', action='store_true', help='Drop existing data before loading')
        parser.add_argument('-b', '--batch-size', help='Batch size for bulk create', type=int, default=500)
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