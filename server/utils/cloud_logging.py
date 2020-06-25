import json
import os
import logging

USE_GOOGLE_LOGGING = False

loggers = {}

severities = ['DEFAULT', 'DEBUG', 'INFO', 'NOTICE', 'WARNING', 'ERROR', 'CRITICAL', 'ALERT', 'EMERGENCY']

DEFAULT = 'DEFAULT'
DEBUG = 'DEBUG'
INFO = 'INFO'
NOTICE = 'NOTICE'
WARNING = 'WARNING'
ERROR = 'ERROR'
EXCEPTION = 'EXCEPTION'
CRITICAL = 'CRITICAL'
ALERT = 'ALERT'
EMERGENCY = 'EMERGENCY'


class Logger:
    logger = None

    def log_to_default(self, msg, severity, **kw):
        dispatch_on_severity = {DEFAULT: self.logger.info,
                                DEBUG: self.logger.debug,
                                INFO: self.logger.info,
                                NOTICE: self.logger.info,
                                WARNING: self.logger.warning,
                                ERROR: self.logger.error,
                                EXCEPTION: self.logger.exception,
                                CRITICAL: self.logger.critical,
                                ALERT: self.logger.critical,
                                EMERGENCY: self.logger.critical}
        log = dispatch_on_severity.get(severity, self.logger.info)
        log(msg, **kw)

    def log_struct_to_default(self, severity, info, client=None, **kw):
        self.log_to_default(json.dumps(info), severity, **kw)

    def log_to_production(self, severity='DEFAULT', msg='', request=None, **kw):
        info = dict()
        if request:
            trace_header = request.headers.get('X-Cloud-Trace-Context')
            if trace_header:
                trace = trace_header.split('/')
                info['logging.googleapis.com/trace'] = (
                    f"projects/datafeed-247709/traces/{trace[0]}")
        info['message'] = msg
        info['severity'] = severity
        self.log_struct(severity, info, **kw)

    def log_text_to_std(self, severity='DEFAULT', msg='', request=None, **kw):
        self.log_to_default(msg, severity, **kw)

    def __init__(self, name=None, use_google_logging=False, is_production=False):
        if name:
            self.name = name
        else:
            self.name = __name__
        if use_google_logging:
            import google.cloud
            self.logger = google.cloud.logging.Client().logger('datafeed')
            self.log_struct = self.logger.log_struct
        else:
            self.logger = logging.getLogger('datafeed')
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s | %(name)s | %(levelname)s | %(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
            self.log_struct = self.log_struct_to_default
        if is_production:
            self.log = self.log_to_production
            self.logger.setLevel(logging.WARNING)
        else:
            self.log = self.log_text_to_std
            self.logger.setLevel(logging.DEBUG)

    def log(self, severity='DEFAULT', msg='', request=None, exc_info=False, **kw):
        raise NotImplementedError('this method should be set on __init__')

    def debug(self, msg, **kw):
        self.log(DEBUG, msg, **kw)

    def info(self, msg, **kw):
        self.log(INFO, msg, **kw)

    def warning(self, msg, **kw):
        self.log(WARNING, msg, **kw)

    def error(self, msg, **kw):
        self.log(ERROR, msg, **kw)

    def critical(self, msg, **kw):
        self.log(CRITICAL, msg, **kw)

    def exception(self, msg, **kw):
        self.log(EXCEPTION, msg, **kw)


def getLogger(name=None, use_google_logging=None, is_production=None):
    global loggers
    if not name:
        name = __name__
    if not use_google_logging:
        use_google_logging = USE_GOOGLE_LOGGING
    if not is_production:
        is_production = os.environ.get('ENVIRONMENT', None) == 'PRODUCTION'
    if not name in loggers:
        loggers[name] = Logger(name, use_google_logging, is_production)
    return loggers[name]


logger = getLogger('root')

if __name__ == '__main__':
    # Instantiates a client
    import wsgi
    logger = Logger('datafeed', False, False)

    logger.debug('THIS IS DEBUG')
    logger.log(INFO, 'this is some info')
    logger.error('this is a django error')
    logger.warning('this is a django warning')
    logger.info('this is a django info')
    try:
        raise NotImplementedError('raising an exception')
    except NotImplementedError as e:
        logger.exception('an exception was caught')
