from django.forms import Form, CharField, BooleanField, IntegerField, TimeField, DateField, FloatField, ModelForm
from django.forms.widgets import TextInput, NumberInput, TimeInput, DateInput

from datetime import datetime, timedelta

from app.models import FundClassification


def create_widget(cls, placeholder=None, readonly=False):
    attrs = {"class": "form-control form-control-user",
             "placeholder": placeholder}
    if readonly:
        attrs["readonly"] = "readonly"
    return cls(attrs=attrs)


today = datetime.today()
initial_end_date = datetime(2019, 5, 31)#today - timedelta(days=1)
initial_start_date = initial_end_date - timedelta(days=30)


class FundClassificationForm(ModelForm):

    INITIALS_US = {'fund': 'HSBAMCA LN Equity',
                   'index': 'SPX Index',
                   'fx': 'GBPUSD Curncy',
                   'has_currency_hedge': False,
                   'index_offset': -1,
                   'fx_time': '12:00',
                   'start_date': initial_start_date,
                   'end_date': initial_end_date,
                   'include_holidays': False,
                   'approved': False}

    INITIALS_JP = {'fund': 'HSBJAPA LN Equity',
                   'index': 'TPX Index',
                   'fx': 'GBPJPY Curncy',
                   'has_currency_hedge': False,
                   'index_offset': 0,
                   'fx_time': '12:00',
                   'start_date': initial_start_date,
                   'end_date': initial_end_date,
                   'include_holidays': False,
                   'approved': False}

    DISPATCH_INITIALS = {'US': INITIALS_US, 'JP': INITIALS_JP}

    def __init__(self, *args, geo=None, **kwargs):
        super().__init__(*args, **kwargs)
        if geo in self.DISPATCH_INITIALS:
            initial_values = self.DISPATCH_INITIALS[geo]
            for field, value in initial_values.items():
                self.fields[field].initial = value

    class Meta:
        model = FundClassification
        fields = ('fund', 'index', 'fx', 'has_currency_hedge', 'index_offset', 'fx_time',
                  'start_date', 'end_date', 'include_holidays', 'rsq', 'beta', 'approved')
        widgets = {'fund': create_widget(TextInput, placeholder="fund"),
                   'index': create_widget(TextInput, placeholder="Index"),
                   'fx': create_widget(TextInput, placeholder="FX"),
                   'index_offset': create_widget(NumberInput, placeholder="Index Offset"),
                   'fx_time': create_widget(TimeInput, placeholder="FX Valuation Time"),
                   'start_date': create_widget(DateInput, placeholder="Start Date"),
                   'end_date': create_widget(DateInput, placeholder="End Date"),
                   'rsq': create_widget(NumberInput, placeholder='Rsq', readonly=True),
                   'beta': create_widget(NumberInput, placeholder="Beta", readonly=True)}
        labels = {'has_currency_hedge': 'Currency Hedge?',
                  'index_offset': 'Index Day Offset',
                  'include_holidays': "Include Holidays?",
                  'approved': "Approved?"}
