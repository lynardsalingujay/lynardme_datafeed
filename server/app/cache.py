from memoize import memoize, Memoizer


def get(*args, **kwargs):
    from django.core.cache import cache
    return cache.get(*args, **kwargs)


def set(*args, **kwargs):
    from django.core.cache import cache
    return cache.set(*args, **kwargs)


def cache_result(timeout_s, override_key=None, version=None, validate=None):
    def decorator(f):
        def _f(*args, **kwargs):
            if override_key is None:
                key = memoize_cache_key(f, *args, **kwargs)
            else:
                key = override_key
            value = get(key, default=None, version=version)
            if value is None:
                value = f(*args, **kwargs)
                if validate is None or validate(value):
                    set(key, value, timeout_s, version=version)
            return value
        _f.original_function = f
        return _f
    return decorator


memoizer = Memoizer()


def memoize_cache_key(f, *args, **kwargs):
    make_cache_key = memoizer._memoize_make_cache_key()
    cache_key = make_cache_key(f, *args, **kwargs)
    return cache_key


def make_key(key, key_prefix, version):
    return '%s:%s:%s' % (key_prefix, version, key)