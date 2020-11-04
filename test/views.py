import os
import json

import pandas as pd
from aiohttp import web
import sqlalchemy as db
from .db_readers import (
    filter_unique_field_pd,
    USERS_TABLE,
    db_engine,
    drop_record,
    update_record,
    get_record,
    get_records
)


async def file_upload(request):
    allowed_file_types = ['.xls', '.xlsx', '.csv']
    data = await request.post()
    uploaded_file = data.get('file')

    if not uploaded_file:
        raise web.HTTPBadRequest(text='File required.')

    _, extension = os.path.splitext(uploaded_file.filename.lower())

    if extension not in allowed_file_types:
        raise web.HTTPBadRequest(text=f'Allowed file types {", ".join(allowed_file_types)}')

    pandas_file_reader = {
        '.xls': pd.read_excel,
        '.xlsx': pd.read_excel,
        '.csv': pd.read_csv,
    }.get(extension, pd.read_csv)
    df = pandas_file_reader(uploaded_file.file)
    df = df.astype(str)
    df = df.groupby('username', as_index=False).first()
    df = filter_unique_field_pd(df, 'username')
    df.to_sql(
        USERS_TABLE,
        db_engine,
        index=False,
        if_exists='append',
        dtype={
            "username": db.String(200),
            "first_name": db.String(200),
            "last_name": db.String(200),
            "email": db.String(200),
            "phone": db.String(200),

        }
    )
    return web.Response(text=f"Items uploaded: {df.shape[0]}")


async def users(request):
    count, records = get_records()
    data = {
        'count': count,
        'users': [r._asdict() for r in records]
    }
    return web.json_response(data)


async def user_delete(request):
    username = request.match_info.get('username')
    if get_record('username', username):
        drop_record('username', username)
        return web.Response(text=f"Username {username} deleted.")
    return web.Response(text=f"Username {username} not found.")


async def user_update(request):
    username = request.match_info.get('username')
    if get_record('username', username):
        try:
            data = await request.json()
        except json.decoder.JSONDecodeError:
            raise web.HTTPBadRequest(text='Invalid data.')
        new_username = data.get('username')
        if new_username and username != new_username and get_record('username', new_username):
            raise web.HTTPBadRequest(text=f'Username {new_username} exists.')
        update_record('username', username, data)
        return web.Response(text=f"Username {username} updated.")
    return web.Response(text=f"Username {username} not found.")
