from django.shortcuts import render
from django.views.generic import TemplateView
from django.views.generic.list import ListView
from django.views.generic.edit import FormView
from django.contrib.auth.mixins import LoginRequiredMixin
from trading.fund_classifier import calculate_fund_classification

from trading.mft import signal_data, contract_information
from pandas import DataFrame
from trading.research.mft_backtest import get_latest_sim_result

from utils import Message, swallow_and_log_exception
from data.parse import FileParser

from abc import abstractmethod
from datetime import datetime
from app.cache import cache_result

from app.forms import FundClassificationForm


def percentage_formatter(number):
    return "{0:.2f}%".format(number * 100)


def float_formatter(number):
    return "{0:,.2f}".format(number)


class AuthenticatedView(LoginRequiredMixin, TemplateView):

    template_name = "authenticated.html"


class FileUploadView(LoginRequiredMixin, TemplateView):

    template_name = "upload_files.html"

    @swallow_and_log_exception(default=('there was an exception while processing the file', 'error'))
    def handle_file_upload(self, request):
        if 'myfile' in request.FILES:
            _, text = FileParser.parse_and_save(request.FILES['myfile'], save_new=True)
            if text == "":
                return "Processed: " + str(request.FILES['myfile']), 'success'
            else:
                return text, 'error'
        else:
            return "Please select a valid file", 'error'

    def post(self, request):
        message, status = self.handle_file_upload(request)
        return render(request, self.template_name, {'message': message, 'status': status})


class TableView(LoginRequiredMixin, ListView):

    template_name = "table.html"
    classes = ["mb-0", "table", "table-striped"]
    columns = None
    formatters = {}
    table_id = "dataTable"
    table_id_2 = "dataTableTwo"
    title = ""
    caption = ""

    def get_queryset(self):
        return None

    def get_context_object_name(self, object_list):
        return None

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context["table"] = self.get_html_table()
        context["title"] = self.title
        context["caption"] = self.caption
        return context

    @abstractmethod
    def get_dataframe(self):
        pass

    def get_html_table(self):
        df = self.get_dataframe()
        return df.to_html(columns=self.columns,
                          formatters=self.formatters,
                          classes=self.classes,
                          table_id=self.table_id,
                          border=0,
                          justify='left',
                          index=False)


@cache_result(24 * 60 * 60, validate=lambda x: x != float('nan'))
def get_vix():
    from data.http.exante import ExanteClient
    exante_client = ExanteClient.create(status="live")
    data = exante_client.latest_price('VIX.INDEX')
    return data['close']


class SignalsView(TableView):

    template_name = 'signals.html'
    columns = ["Index", "Future", "Future Price", "Index Price", "Fair Spread", "Gross Signal", "Net Signal", "Time"]
    formatters = {"Future Price": float_formatter,
                  "Index Price": float_formatter,
                  "Fair Spread": float_formatter,
                  "Gross Signal": percentage_formatter,
                  "Net Signal": percentage_formatter}
    title = "Signals"
    caption = "Signals per index"
    threshold_caption = 'Vol-scaled thresholds based on sim optimisation'

    @staticmethod
    def get_threshold_dataframe():
        sim_result = get_latest_sim_result()
        vix_price = get_vix()
        in_threshold, out_threshold = {}, {}
        in_threshold['direction'] = 'in'
        in_threshold['geo'] = 'all'
        in_threshold['alpha'] = sim_result['open_threshold_alpha']
        in_threshold['beta'] = sim_result['open_threshold_beta']
        in_threshold['VIX'] = vix_price
        in_threshold['value'] = sim_result['open_threshold_alpha'] + sim_result['open_threshold_beta'] * vix_price
        out_threshold['direction'] = 'out'
        out_threshold['geo'] = 'all'
        out_threshold['alpha'] = sim_result['close_threshold_alpha']
        out_threshold['beta'] = sim_result['close_threshold_beta']
        out_threshold['VIX'] = vix_price
        out_threshold['value'] = sim_result['close_threshold_alpha'] + sim_result['close_threshold_beta'] * vix_price
        return DataFrame.from_records([in_threshold, out_threshold])

    @swallow_and_log_exception(default='there was an error calculating the thresholds, please try again later.')
    def get_threshold_table(self):
        df = SignalsView.get_threshold_dataframe()
        formatters = {'alpha': percentage_formatter,
                      'beta': percentage_formatter,
                      'VIX': float_formatter,
                      'value': percentage_formatter}
        columns = ['direction', 'geo', 'alpha', 'beta', 'VIX', 'value']
        return df.to_html(columns=columns,
                          formatters=formatters,
                          classes=self.classes,
                          table_id=self.table_id_2,
                          border=0,
                          justify='left',
                          index=False)

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context['threshold_table'] = self.get_threshold_table()
        context['threshold_caption'] = self.threshold_caption
        return context

    @swallow_and_log_exception(default=DataFrame())
    def get_dataframe(self):
        return signal_data()


class ContractsView(TableView):

    columns = ["Name", "Country", "Exchange", "Exante ID", "Currency", "Expiration", "Contract Size (£)", "As Of"]
    formatters = {"Contract Size (£)": float_formatter}
    title = "Futures Contracts"
    caption = "Refresh the page to calculate contract size again"

    def get_dataframe(self):
        return contract_information()


class ModelView(TableView):

    model = None

    @swallow_and_log_exception(default=DataFrame())
    def get_dataframe(self):
        from app.models import to_dataframe
        query_set = self.model.objects.all()
        df = to_dataframe(query_set)
        return df


class FundClassifierView(LoginRequiredMixin, FormView):

    geo = None
    template_name = "fund_classifier.html"
    form_class = FundClassificationForm

    def get_form_kwargs(self):
        kwargs = super().get_form_kwargs()
        kwargs['geo'] = self.geo
        return kwargs

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context['geo'] = self.geo
        return context

    def post(self, request, *args, **kwargs):
        from bokeh.embed import components
        from math import floor
        request.POST._mutable = True
        fund_classification = FundClassificationForm(request.POST)
        if 'calculate' in request.POST:
            _mutable = fund_classification.data._mutable
            fund_classification.data._mutable = True
            is_valid = fund_classification.is_valid()
            data = fund_classification.clean()
            rsq, beta, chart = calculate_fund_classification(**data)
            script, div = components(chart)
            fund_classification.data['rsq'] = floor(rsq*100)/100
            fund_classification.data['beta'] = floor(beta*100)/100
            fund_classification.data._mutable = _mutable
            return render(request, self.template_name, {'form': fund_classification,
                                                        'div': div,
                                                        'script': script,
                                                        'geo': self.geo})
        elif 'save' in request.POST:
            obj = fund_classification.save(commit=False)
            obj.as_of = datetime.now()
            obj.save()
            return render(request, self.template_name, {'form': fund_classification, 'geo': self.geo})
        else:
            raise ValueError('FundClassifierView: can only calculate or save in this view')


class FundClassifierViewUS(FundClassifierView):
    geo = 'US'


class FundClassifierViewJP(FundClassifierView):
    geo = 'JP'
