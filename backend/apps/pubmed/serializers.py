from rest_framework import serializers

from . import models


class PubmedArticleSerializer(serializers.ModelSerializer):
    class Meta:
        model = models.PubmedArticle
        fields = [
            'pmid',
            'title',
            'abstract',
            'year',
            'pubmed_pubdate',
            'factor',
            'jcr',
            'journal',
            'pagination',
            'volume',
            'authors',
            'doi',
            'pmc',
        ]
