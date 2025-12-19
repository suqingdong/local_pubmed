import time
import math
import json
from loguru import logger

import concurrent.futures
from django.core.management.base import BaseCommand
from django.db import transaction, connection

from pubmed.models import PubmedArticle
import utils


class Command(BaseCommand):
    help = 'Embedding PubMed Database'

    def add_arguments(self, parser):
        parser.add_argument('-b', '--batch-size', help='Batch size for bulk create', type=int, default=1000)
        parser.add_argument('-o', '--output', help='Output file', default='pubmed_embeddings.jl.gz')
        parser.add_argument('-n', '--num-threads', help='Concurrency for embedding requests', type=int, default=8)

    def handle(self, *args, **kwargs):
        batch_size = kwargs['batch_size']
        output = kwargs['output']
        num_threads = kwargs['num_threads']

        start_time = time.time()

        qs = PubmedArticle.objects.all().values('pmid', 'title', 'abstract')
        total = qs.count()
        logger.info(f"Total articles to process: {total}")

        def get_batch_articles():
            last_pmid = 0
            while True:
                qs = (
                    PubmedArticle.objects
                    .filter(pmid__gt=last_pmid)
                    .order_by('pmid')
                    .values('pmid', 'title', 'abstract')[:batch_size]
                )
                rows = list(qs)
                if not rows:
                    break

                yield rows
                last_pmid = rows[-1]['pmid']


        # 定义一个函数，用于在单独的线程中处理单个批次
        def process_batch(batch_data):
            embeddings = utils.get_embeddings('text-embedding-3-small')
            texts = [f"{row['title']} {row['abstract']}" for row in batch_data]
            while True:
                try:
                    vectors = embeddings.embed_documents(texts)
                    break
                except Exception as e:
                    logger.error(f"Error in embedding: {e}")
                    time.sleep(10)
            
            return [{'pmid': r['pmid'], 'vec': v} for r, v in zip(batch, vectors)]
 

        # 使用 ThreadPoolExecutor 进行并发处理
        completed = 0
        total_batches = math.ceil(total / batch_size)
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = set()

            with utils.safe_open(output, 'wb') as out:
                for n, batch in enumerate(get_batch_articles(), 1):
                    logger.info(f"submit batch {n}...")
                    futures.add(executor.submit(process_batch, batch))

                    if len(futures) >= num_threads * 2:
                        done, futures = concurrent.futures.wait(
                            futures, return_when=concurrent.futures.FIRST_COMPLETED
                        )
                        for f in done:
                            completed += 1
                            logger.info(f"[{completed}/{total_batches}] batch finished")
                            for d in f.result():
                                out.write((json.dumps(d) + '\n').encode())

                for f in concurrent.futures.as_completed(futures):
                    completed += 1
                    for d in f.result():
                        logger.info(f"[{completed}/{total_batches}] batch finished")
                        out.write((json.dumps(d) + '\n').encode())

        logger.info(f"Finished in {time.time() - start_time:.2f} seconds")