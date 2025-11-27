from django.urls import path, include

from rest_framework.viewsets import ModelViewSet
from rest_framework.routers import DefaultRouter
from rest_framework.views import APIView


from . import views

urlpatterns = []


for key, value in views.__dict__.items():
    if hasattr(value, '__route__') and value.__base__ in [APIView]:
        urlpatterns.append(path(f'{value.__route__}/', value.as_view(), name=value.__route__))

router = DefaultRouter()

for key, value in views.__dict__.items():
    if hasattr(value, '__route__') and value.__base__ in [ModelViewSet]:
        router.register(value.__route__, value, basename=value.__route__)

urlpatterns += [
    path('', include(router.urls)),
]
