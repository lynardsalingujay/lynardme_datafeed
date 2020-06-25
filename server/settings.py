import os
from datetime import timedelta

ENVIRONMENT = os.environ.get('ENVIRONMENT', None)

allowed_environments = ['PRODUCTION', 'BUILD', 'DEV']

default_environment = 'DEV'

if ENVIRONMENT not in allowed_environments:
    print('ENVIRONMENT not set, defaulting to ' + str(default_environment))
    ENVIRONMENT = default_environment

# Build paths inside the project like this: os.path.join(BASE_DIR, ...)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.path.join(BASE_DIR, 'opps', 'logging_creds.json')

# Quick-start development settings - unsuitable for production
# See https://docs.djangoproject.com/en/1.11/howto/deployment/checklist/

# SECURITY WARNING: keep the secret key used in production secret!

SECRET_KEY = os.environ.get("SECRET_KEY", "some kind of secret key")

# SECURITY WARNING: don't run with debug turned on in production!

debug_map = {'DEV': True, 'BUILD': True, 'PRODUCTION': True}  # this is insecure

DEBUG = debug_map[ENVIRONMENT]

ALLOWED_HOSTS = ["*"]

# Application definition

INSTALLED_APPS = [
    'app.apps.MyAppConfig',
    'django.contrib.admin',
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.messages',
    'django.contrib.staticfiles',
    'rest_framework',
    'rest_framework.authtoken',
    'corsheaders',
    'behave_django'
]

MIDDLEWARE = [
    'corsheaders.middleware.CorsMiddleware',
    'django.middleware.security.SecurityMiddleware',
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.common.CommonMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',
    'django.middleware.clickjacking.XFrameOptionsMiddleware',
    'django_cprofile_middleware.middleware.ProfilerMiddleware'
]

DJANGO_CPROFILE_MIDDLEWARE_REQUIRE_STAFF = False

ROOT_URLCONF = 'app.urls'

WSGI_APPLICATION = 'wsgi.application'

# Database
# https://docs.djangoproject.com/en/1.11/ref/settings/#databases

# https://github.com/GoogleCloudPlatform/python-docs-samples/blob/master/appengine/standard/django/server/settings.py
database_map = {'PRODUCTION': {'default': {'ENGINE': 'django.db.backends.postgresql',
                                           'NAME': 'postgres',
                                           'USER': 'postgres',
                                           'PASSWORD': 'cwjwrHFvFHuN3LMz',
                                           'HOST': '/cloudsql/datafeed-247709:asia-northeast1:shiny-production',
                                           'PORT': 5432}},
                'BUILD': {'default': {'ENGINE': 'django.db.backends.postgresql',
                                      'NAME': 'postgres',
                                      'USER': 'postgres',
                                      'PASSWORD': 'cwjwrHFvFHuN3LMz',
                                      'HOST': '/cloudsql/datafeed-247709:asia-northeast1:shiny-production'}},
                'DEV': {'default': {'ENGINE': 'django.db.backends.sqlite3',
                                    'NAME': os.environ.get('DEV_DB', None)}}}


DATABASES = database_map[ENVIRONMENT]


print('ENVIRONMENT=' + ENVIRONMENT + ', DATABASES='+str(DATABASES))

# Password validation
# https://docs.djangoproject.com/en/1.11/ref/settings/#auth-password-validators

AUTH_PASSWORD_VALIDATORS = [
    {
        'NAME': 'django.contrib.auth.password_validation.UserAttributeSimilarityValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.MinimumLengthValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.CommonPasswordValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.NumericPasswordValidator',
    },
]

# Internationalization
# https://docs.djangoproject.com/en/1.11/topics/i18n/

LANGUAGE_CODE = 'en-us'

TIME_ZONE = 'UTC'

USE_I18N = True

USE_L10N = True

USE_TZ = True

# Static files (CSS, JavaScript, Images)
# https://docs.djangoproject.com/en/1.11/howto/static-files/

# this is not secure but hard to get working with Google bucket

STATIC_URL = '/static/'

STATIC_ROOT = '/var/www/shiny/static/'

STATICFILES_DIRS = [os.path.join(BASE_DIR, 'client', 'static')]

LOGIN_URL = '/login'

LOGIN_REDIRECT_URL = '/index'

CACHES = {
    'default': {
        'BACKEND': 'django.core.cache.backends.db.DatabaseCache',
        'LOCATION': 'database_cache',
    }
}

LOGGING_CONFIG = None

REST_FRAMEWORK = {
    'DEFAULT_AUTHENTICATION_CLASSES': (
        'rest_framework_simplejwt.authentication.JWTAuthentication',),
    'DEFAULT_PERMISSION_CLASSES': [
        'rest_framework.permissions.IsAuthenticated',
    ]
}

SIMPLE_JWT = {
    'ACCESS_TOKEN_LIFETIME': timedelta(days=1),
    'REFRESH_TOKEN_LIFETIME': timedelta(days=10),
}

TEMPLATES = [
    {
        'BACKEND': 'django.template.backends.django.DjangoTemplates',
        'DIRS': [os.path.join(BASE_DIR, 'client', 'templates')],
        'APP_DIRS': False,
        'OPTIONS': {
            'context_processors': [
                'django.template.context_processors.debug',
                'django.template.context_processors.request',
                'django.contrib.auth.context_processors.auth',
                'django.contrib.messages.context_processors.messages',
            ],
        },
    },
]

CORS_ORIGIN_ALLOW_ALL = True

CORS_ORIGIN_WHITELIST = ()
