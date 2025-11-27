from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework.permissions import IsAuthenticated

from pgvector.django import CosineDistance, L2Distance

from utils.llm import get_embeddings
from pubmed.models import PubmedArticle
from pubmed.serializers import PubmedArticleSerializer
from pubmed.permissions import APIKeyPermission


def vector_search(queryset, vector, top_k=10, threshold=None, start=0):
    qs = queryset.annotate(distance=CosineDistance('title_abstract_vector', vector))
    if threshold is not None:
        qs = qs.filter(distance__lte=threshold)
    qs = qs.order_by('distance')[start:top_k]
    return qs


class PubmedSearchView(APIView):

    __route__ = 'search'
    embeddings = get_embeddings()

    permission_classes = [APIKeyPermission]

    def search(self, payload):

        query = payload.get('q', '')
        year = payload.get('year', None)
        factor = payload.get('factor', None)
        top_k = int(payload.get('top_k', 10))
        start = int(payload.get('start', 0))

        if not query.strip():
            return Response({'success': False, 'message': 'q is required!'})

        vector = self.embeddings.embed_query(query)

        queryset = PubmedArticle.objects.all()
        if year is not None:
            queryset = queryset.filter(year__gte=int(year))
        if factor is not None:
            queryset = queryset.filter(factor__gte=float(factor))

        results = vector_search(queryset, vector, top_k=top_k, start=start)
        data = PubmedArticleSerializer(results, many=True).data

        return Response({'success': True, 'query': query, 'data': data})

    def get(self, request, *args, **kwargs):
        return self.search(request.query_params)

    def post(self, request, *args, **kwargs):
        return self.search(request.data)
