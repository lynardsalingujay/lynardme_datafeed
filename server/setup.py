from setuptools import setup, find_packages

import app
    
setup(
    name = "server",
    version = app.__version__,
    author = "Alexander Berkley",
    author_email = "alexander.berkley@gmail.com",
    description = ("Collects data"),
    license = None,
    keywords = [],
    url = "https://github.com/aberkley/datafeed",
    packages=find_packages(),
    long_description="",
    classifiers=["Development Status :: 3 - Alpha"],
    install_requires = [
        "pandas==0.25.1",
        "django==2.2.6",
        "lxml==4.4.1",
        "requests==2.22.0",
        "xlrd==1.2.0",
        "django-memoize==2.2.0",
        'djangorestframework==3.10.3',
        'djangorestframework_simplejwt==4.3.0',
        'psycopg2==2.8.3',
        'django-cors-headers==3.1.1',
        'PyPDF2==1.26.0',
        'django-crispy-forms==1.9.1',
        'bokeh==2.0.2',
        'pandas-market-calendars',
        'sqlalchemy',
        'django-pandas',
        'behave-django',
        'google-cloud-logging'
    ])
