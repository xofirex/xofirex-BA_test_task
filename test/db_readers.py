import pandas as pd

from .db import db_engine, USERS_TABLE, user_table, with_session


def get_records_pd(table):
    df_from_db = pd.read_sql_table(table, db_engine)
    return df_from_db


def filter_unique_field_pd(df, field):
    df_from_db = get_records_pd(USERS_TABLE)
    df = df[~getattr(df, field).isin(getattr(df_from_db, field))]
    return df


def q_table_by_key(field, key):
    return getattr(user_table.c, field) == key


@with_session
def get_records(session):
    query = session.query(user_table)
    return query.count(), query.all()


@with_session
def get_record(session, field, key):
    return session.query(user_table).filter(q_table_by_key(field, key)).first()


@with_session
def drop_record(session, field, key):
    update_query = user_table.delete().where(q_table_by_key(field, key))
    session.execute(update_query)


@with_session
def update_record(session, field, key, data):
    update_query = user_table.update().where(q_table_by_key(field, key)).values(data)
    session.execute(update_query)
