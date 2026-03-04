import time
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework.permissions import IsAuthenticated
from django.db import transaction, connection

from pgvector.django import CosineDistance, L2Distance

from utils.llm import get_embeddings
from pubmed.models import PubmedArticle
from pubmed.serializers import PubmedArticleSerializer
from pubmed.permissions import APIKeyPermission
from pubmed.utils.search import hybrid_search



# 接口维护中响应
def maintenance_response():
    return Response({
        'success': False,
        'message': 'This API is under maintenance. Please try again later.',
    })


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

        # return Response({
        #     'success': False,
        #     'message': 'This API is under maintenance. Please try again later.',
        # })

        start_time = time.time()

        query = payload.get('q', '').strip()
        pmid_str = payload.get('id', '').strip()
        year_start = payload.get('year_start', 2021)
        year_end = payload.get('year_end', None)
        factor_min = payload.get('factor_min', None)
        factor_max = payload.get('factor_max', None)
        top_k = int(payload.get('top_k', 10))
        start = int(payload.get('start', 0))

        ef_search = 40

        # top_k限制在100以内
        if top_k > 100:
            top_k = 100

        # 限制2021年之后的文章
        if int(year_start) < 2021:
            year_start = 2021

        if not query and not pmid_str:
            return Response({'success': False, 'message': 'q or id is required!'})
        
        base_qs = PubmedArticle.objects.all()

        if pmid_str:
            pmid_list = [int(pmid) for pmid in str(pmid_str).split(',') if str(pmid).strip().isdigit()]
            base_qs = base_qs.filter(pmid__in=pmid_list)
            results = base_qs.all()

        else:
            has_filter = False
            if year_start:
                base_qs = base_qs.filter(year__gte=int(year_start))
                has_filter = True
            if year_end:
                base_qs = base_qs.filter(year__lte=int(year_end))
                has_filter = True
            if factor_min:
                base_qs = base_qs.filter(factor__gte=float(factor_min))
                has_filter = True
            if factor_max:
                base_qs = base_qs.filter(factor__lte=float(factor_max))
                has_filter = True

            with transaction.atomic():
                with connection.cursor() as cursor:
                    ef_search = 80 if has_filter else 40
                    cursor.execute('SET LOCAL hnsw.ef_search=%s;', [ef_search])

                    results = hybrid_search(
                        query,
                        base_qs,
                        top_k=top_k,
                        start=start,
                        vector_topn=200 if has_filter else 100,
                    )

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

        elapsed_time = time.time() - start_time
        
        return Response({
            'success': True,
            'query': query_dict,
            'data': data,
            'elapsed_time': f'{elapsed_time:.2f}s',
        })

    def get(self, request, *args, **kwargs):
        return self.search(request.query_params)

    def post(self, request, *args, **kwargs):
        return self.search(request.data)

        
