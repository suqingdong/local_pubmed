from django.db import models
from pgvector.django import VectorField



class PubmedArticle(models.Model):
    pmid = models.IntegerField(primary_key=True, verbose_name='PMID')
    title = models.CharField(max_length=2000, verbose_name='Title', null=True, blank=True)
    abstract = models.TextField(verbose_name='Abstract', null=True, blank=True)
    journal = models.CharField(max_length=500, verbose_name='Journal', null=True, blank=True)
    med_abbr = models.CharField(max_length=500, verbose_name='Med Abbreviation', null=True, blank=True)
    iso_abbr = models.CharField(max_length=500, verbose_name='ISO Abbreviation', null=True, blank=True)
    pubdate = models.CharField(max_length=500, verbose_name='Publication Date', null=True, blank=True)
    pubmed_pubdate = models.DateField(verbose_name='Pubmed Publication Date', null=True, blank=True)
    pmc = models.CharField(max_length=500, verbose_name='PMC', null=True, blank=True)
    issn = models.CharField(max_length=500, verbose_name='ISSN', null=True, blank=True)
    e_issn = models.CharField(max_length=500, verbose_name='E-ISSN', null=True, blank=True)
    doi = models.CharField(max_length=500, verbose_name='DOI', null=True, blank=True)
    year = models.IntegerField(verbose_name='Year', null=True, blank=True)
    pagination = models.CharField(max_length=500, verbose_name='Pagination', null=True, blank=True)
    volume = models.CharField(max_length=500, verbose_name='Volume', null=True, blank=True)
    issue = models.CharField(max_length=500, verbose_name='Issue', null=True, blank=True)
    pub_status = models.CharField(max_length=500, verbose_name='Publication Status', null=True, blank=True)
    authors = models.JSONField(verbose_name='Authors', null=True, blank=True)
    keywords = models.JSONField(verbose_name='Keywords', null=True, blank=True)
    pub_types = models.JSONField(verbose_name='Publication Types', null=True, blank=True)
    author_mail = models.JSONField(verbose_name='Author Mails', null=True, blank=True)
    author_first = models.CharField(max_length=500, verbose_name='Author First', null=True, blank=True)
    author_last = models.CharField(max_length=500, verbose_name='Author Last', null=True, blank=True)
    affiliations = models.JSONField(verbose_name='Affiliations', null=True, blank=True)

    abstract_cn = models.TextField(verbose_name='Abstract CN', null=True, blank=True)

    factor = models.FloatField(verbose_name='Factor', null=True, blank=True)
    jcr = models.CharField(max_length=10, verbose_name='JCR', null=True, blank=True)

    title_abstract_vector = VectorField(dimensions=3072, verbose_name='Title Abstract Vector', null=True, blank=True)

    class Meta:
        verbose_name = 'Pubmed Article'
        verbose_name_plural = 'Pubmed Articles'
        ordering = ['-pubmed_pubdate']
        db_table = 'pubmed_articles'

    def __str__(self):
        return f'{self.pmid} - {self.title}'
