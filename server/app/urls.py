from django.conf.urls import url, include
from django.contrib import admin
from django.contrib.staticfiles.urls import staticfiles_urlpatterns
from django.urls import path
from  django.contrib.auth.views import LoginView
from django.views.generic import RedirectView
from rest_framework.decorators import api_view
from rest_framework_simplejwt.views import TokenObtainPairView, TokenRefreshView, TokenVerifyView

from app import api_views
from app.api_views import handle_file_upload, handle_json_model
from app.page_views import *
from app.models import CashMovement, Transaction
from django.conf import settings
from django.conf.urls.static import static

urlpatterns = [
    path('', RedirectView.as_view(pattern_name='index', permanent=False)),
    path('login/', LoginView.as_view(template_name='login.html'), name='login'),
    path('index/', AuthenticatedView.as_view(template_name='index.html'), name='index'),
    path('fund_classifier/', FundClassifierView.as_view(), name='fund_classifier'),
    path('fund_classifier/us', FundClassifierViewUS.as_view(), name='fund_classifier_us'),
    path('fund_classifier/jp', FundClassifierViewJP.as_view(), name='fund_classifier_jp'),
    path('mft_signals/', SignalsView.as_view(), name='signals'),
    path('contracts/', ContractsView.as_view(), name='contracts'),
    path('upload_files/', FileUploadView.as_view(template_name='upload_files.html'), name='upload_files'),
    path('cash_movements/', ModelView.as_view(model=CashMovement, title="Cash Movements"), name='cash_movements'),
    path('transactions/', ModelView.as_view(model=Transaction, title="Transactions"), name='transactions'),
    path('admin/', admin.site.urls),
    path('api/token/obtain/', TokenObtainPairView.as_view(), name='token_obtain_pair'),
    path('api/token/refresh/', TokenRefreshView.as_view(), name='token_refresh'),
    path('api/token/verify/', TokenVerifyView.as_view(), name='token_verify'),
    url(r'^api-auth/', include('rest_framework.urls', namespace='rest_framework')),
    path('api/<str:status>/<str:response_type>/file_upload', handle_file_upload, name='file_upload'),
    path('api/<str:status>/<str:response_type>/model', handle_json_model, name='handle_json_model'),
    path("api/<str:status>/<str:response_type>/prices/recent/fund/<str:isin>/<str:currency>/", api_views.recent_fund_prices, name="recent_fund_prices"),
    path("api/<str:status>/<str:response_type>/signals", api_views.signals, name="signals"),
    path("api/<str:status>/<str:response_type>/reyl_cash_movements", api_views.reyl_cash_movements, name="reyl_cash_movements"),
    path("api/<str:status>/<str:response_type>/reyl_transactions", api_views.reyl_transactions, name="reyl_transactions"),
    path("api/<str:status>/<str:response_type>/reyl_cash_movement_summary", api_views.reyl_cash_movement_summary, name="reyl_cash_movement_summary"),
    path("api/<str:status>/<str:response_type>/reyl_transactions_with_no_price", api_views.reyl_transactions_with_no_price, name="reyl_transactions_with_no_price"),
    path("api/<str:status>/<str:response_type>/contract_info", api_views.contract_info, name="contract_info"),
]

urlpatterns += static(settings.STATIC_URL, document_root=settings.STATIC_ROOT)