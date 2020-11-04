from .views import file_upload, users, user_delete, user_update


def setup_routes(app):
    app.router.add_post('/users', file_upload, name='csv_upload')
    app.router.add_get('/users', users, name='users')
    app.router.add_delete('/users/{username}', user_delete, name='user_delete')
    app.router.add_put('/users/{username}', user_update, name='user_update')

