from django.db import models


"""
  "pmid",
    "title",
    "abstract",
    "impact_factor",
    "journal",
    "med_abbr",
    "iso_abbr",
    "pubdate",
    "pubmed_pubdate",
    "pmc",
    "issn",
    "e_issn",
    "doi",
    "year",
    "pagination",
    "volume",
    "issue",
    "pub_status",
    "authors",
    "keywords",
    "pub_types",
    "author_mail",
    "author_first",
    "author_last",
    "affiliations"
"""

class PubmedArticle(models.Model):
    pmid = models.IntegerField(primary_key=True, verbose_name='PMID')
    title = models.CharField(max_length=500, verbose_name='Title')
    abstract = models.TextField(verbose_name='Abstract')
    impact_factor = models.FloatField(verbose_name='Impact Factor', null=True, blank=True)
    journal = models.CharField(max_length=100, verbose_name='Journal')
    med_abbr = models.CharField(max_length=100, verbose_name='Med Abbreviation')
    iso_abbr = models.CharField(max_length=100, verbose_name='ISO Abbreviation')
    pubdate = models.CharField(max_length=100, verbose_name='Publication Date')
    pubmed_pubdate = models.DateField(verbose_name='Pubmed Publication Date')
    pmc = models.CharField(max_length=100, verbose_name='PMC', null=True, blank=True)
    issn = models.CharField(max_length=100, verbose_name='ISSN', null=True, blank=True)
    e_issn = models.CharField(max_length=100, verbose_name='E-ISSN', null=True, blank=True)
    doi = models.CharField(max_length=100, verbose_name='DOI', null=True, blank=True)
    year = models.IntegerField(verbose_name='Year')
    pagination = models.CharField(max_length=100, verbose_name='Pagination', null=True, blank=True)
    volume = models.CharField(max_length=100, verbose_name='Volume', null=True, blank=True)
    issue = models.CharField(max_length=100, verbose_name='Issue', null=True, blank=True)
    pub_status = models.CharField(max_length=100, verbose_name='Publication Status', null=True, blank=True)
    authors = models.JSONField(verbose_name='Authors', null=True, blank=True)
    keywords = models.JSONField(verbose_name='Keywords', null=True, blank=True)
    pub_types = models.JSONField(verbose_name='Publication Types', null=True, blank=True)
    author_mail = models.JSONField(verbose_name='Author Mails', null=True, blank=True)
    author_first = models.CharField(max_length=100, verbose_name='Author First', null=True, blank=True)
    author_last = models.CharField(max_length=100, verbose_name='Author Last', null=True, blank=True)
    affiliations = models.JSONField(verbose_name='Affiliations', null=True, blank=True)

    class Meta:
        verbose_name = 'Pubmed Article'
        verbose_name_plural = 'Pubmed Articles'
        ordering = ['pmid']

    def __str__(self):
        return f'{self.pmid} - {self.title}'
    