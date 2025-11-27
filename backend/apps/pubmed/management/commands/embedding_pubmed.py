import time
from loguru import logger

from django.core.management.base import BaseCommand
from django.db import transaction, connection

from pubmed.models import PubmedArticle
import utils


class Command(BaseCommand):
    help = 'Embedding PubMed Database'

    def add_arguments(self, parser):
        parser.add_argument('-b', '--batch-size', help='Batch size for bulk create', type=int, default=1000)

    def handle(self, *args, **kwargs):
        batch_size = kwargs['batch_size']

        start_time = time.time()

        qs = PubmedArticle.objects.filter(title_abstract_vector__isnull=True).values('pmid', 'title', 'abstract')

        total = qs.count()
        logger.info(f"Total articles to process: {total}")

        embeddings = utils.get_embeddings()

        for start in range(0, total, batch_size):
            # 取一批数据
            batch = list(qs[start:start + batch_size])
            texts = [f"{row['title']} {row['abstract']}" for row in batch]

            logger.info(f"Processing batch {start} ~ {start+len(batch)}")

            # --- ⭐ 批量 embeddings (关键优化点) ---
            vectors = embeddings.embed_documents(texts)

            # --- ⭐ 批量更新数据库 (第二关键优化点) ---
            objs = []
            for row, vec in zip(batch, vectors):
                objs.append(
                    PubmedArticle(pmid=row['pmid'], title_abstract_vector=vec)
                )

            with transaction.atomic():
                PubmedArticle.objects.bulk_update(
                    objs,
                    ['title_abstract_vector'],
                    batch_size=2000,   # PostgreSQL 一般没问题
                )

        logger.info(f"Finished in {time.time() - start_time:.2f} seconds")