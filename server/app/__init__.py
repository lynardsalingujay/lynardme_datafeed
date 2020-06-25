__version__ = "0.1.0"

if __name__ == '__main__':
    import wsgi
    from app.views import signals
    from django.test import RequestFactory
    from django.contrib.auth.models import User

    request = RequestFactory().get('/customer/details')
    request.user = User.objects.create_user(username='jacob', email='jacob@…', password='top_secret')
    response = signals(request)
    print(response)