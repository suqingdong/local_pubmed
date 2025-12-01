from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework.permissions import IsAuthenticated

from pgvector.django import CosineDistance, L2Distance

from utils.llm import get_embeddings
from pubmed.models import PubmedArticle
from pubmed.serializers import PubmedArticleSerializer
from pubmed.permissions import APIKeyPermission
from pubmed.utils.hybrid_search import hybrid_search


def vector_search(queryset, vector, top_k=10, threshold=None, start=0):
    qs = queryset.annotate(distance=CosineDistance('title_abstract_vector', vector))
    if threshold is not None:
        qs = qs.filter(distance__lte=threshold)
    qs = qs.order_by('distance')[start:start+top_k]
    # print(qs.query)
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



class PubmedHybridSearchView(APIView):

    __route__ = 'hybrid_search'

    permission_classes = [APIKeyPermission]

    def search(self, payload):
        """混合搜索接口
        
        支持以下参数：
            - q: 查询字符串
            - id: pmid字符串，用逗号分隔
            - year_start: 开始年份
            - year_end: 结束年份
            - factor_min: 最小因子
            - factor_max: 最大因子
            - top_k: 返回结果数量
            - start: 起始位置
        """

        query = payload.get('q', '')
        pmid_str = payload.get('id', '')
        year_start = payload.get('year_start', None)
        year_end = payload.get('year_end', None)
        factor_min = payload.get('factor_min', None)
        factor_max = payload.get('factor_max', None)
        top_k = int(payload.get('top_k', 10))
        start = int(payload.get('start', 0))

        # top_k限制在100以内
        if top_k > 100:
            top_k = 100

        if not query.strip() and not pmid_str.strip():
            return Response({'success': False, 'message': 'q or id is required!'})
        
        base_qs = PubmedArticle.objects.all()

        if pmid_str:
            pmid_list = [int(pmid) for pmid in str(pmid_str).split(',') if str(pmid).strip().isdigit()]
            base_qs = base_qs.filter(pmid__in=pmid_list)
            results = base_qs.all()
        else:
            if year_start:
                base_qs = base_qs.filter(year__gte=int(year_start))
            if year_end:
                base_qs = base_qs.filter(year__lte=int(year_end))
            if factor_min:
                base_qs = base_qs.filter(factor__gte=float(factor_min))
            if factor_max:
                base_qs = base_qs.filter(factor__lte=float(factor_max))
            results = hybrid_search(query, base_qs, top_k=top_k, start=start)

        data = PubmedArticleSerializer(results, many=True).data

        query_dict = {
            'q': query,
            'id': pmid_str,
            'year_start': year_start,
            'year_end': year_end,
            'factor_min': factor_min,
            'factor_max': factor_max,
            'top_k': top_k,
            'start': start,
        }
        
        return Response({'success': True, 'query': query_dict, 'data': data})

    def get(self, request, *args, **kwargs):
        return self.search(request.query_params)

    def post(self, request, *args, **kwargs):
        return self.search(request.data)

        
