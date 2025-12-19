import time
import math
import json
from itertools import islice
from loguru import logger

import concurrent.futures
from django.core.management.base import BaseCommand
from django.db import transaction, connection

from pubmed.models import PubmedArticle
import utils


def read_jsonl_batches(path, batch_size):
    with utils.safe_open(path, 'rb') as f:
        while True:
            lines = list(islice(f, batch_size))
            if not lines:
                break
            yield [json.loads(l) for l in lines]


class Command(BaseCommand):
    help = 'Embedding PubMed Database'

    def add_arguments(self, parser):
        parser.add_argument('-i', '--input-file', help='Input file', required=True)
        parser.add_argument('-b', '--batch-size', help='Batch size for bulk create', type=int, default=2000)

    def handle(self, *args, **kwargs):
        input_file = kwargs['input_file']
        batch_size = kwargs['batch_size']

        start_time = time.time()

        complete_count = 0
        for batch_data in read_jsonl_batches(input_file, batch_size):
            objs = [PubmedArticle(pmid=row['pmid'], title_abstract_vec=row['vec']) for row in batch_data]
            with transaction.atomic():
                PubmedArticle.objects.bulk_update(
                    objs,
                    ['title_abstract_vec'],
                    batch_size=2000,
                )
            complete_count += len(batch_data)
            logger.debug(f'Processed {complete_count} articles')

        logger.info(f"Finished in {time.time() - start_time:.2f} seconds")
