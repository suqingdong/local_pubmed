from celery import shared_task
from pubmed_xml import Pubmed_XML_Parser
from dateutil.parser import parse as date_parse

from pubmed.models import PubmedArticle


@shared_task
def update_pubmed():
    print('>>> updating pubmed ...')
    parser = Pubmed_XML_Parser()
    result = parser.parse('/work/data/pubmed/work/data/2025/baseline/pubmed24n0001.xml.gz')
    for article in result:
        data = article.data
        data['pubmed_pubdate'] = date_parse(article.pubmed_pubdate).strftime('%F')
        obj, created = PubmedArticle.objects.update_or_create(
            pmid=data['pmid'],
            defaults=data
        )
        if created:
            print(f'>>> article created: {article.pmid}')
        else:
            print(f'>>> article updated: {article.pmid}')
