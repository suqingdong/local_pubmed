from rest_framework.permissions import BasePermission
from django.conf import settings


class APIKeyPermission(BasePermission):
    def has_permission(self, request, view):

        if hasattr(settings, 'PUBMED_API_KEY'):
            expected_key = settings.PUBMED_API_KEY
            api_key = request.headers.get('X-API-KEY') or request.query_params.get('api_key')
            return api_key == expected_key

        return True
