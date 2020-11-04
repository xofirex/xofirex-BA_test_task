from .settings import BASE_DIR, get_config
from sqlalchemy import create_engine
import sqlalchemy as db
from sqlalchemy.orm import sessionmaker

import functools


DSN = "postgresql://{user}:{password}@{host}:{port}/{database}"
CONFIG_PATH = BASE_DIR / 'config' / 'test.yaml'
CONFIG = get_config(['-c', CONFIG_PATH.as_posix()])
DB_URL = DSN.format(**CONFIG['postgres'])
db_engine = create_engine(DB_URL)
metadata = db.MetaData()

USERS_TABLE = 'users'
user_table = db.Table(USERS_TABLE, metadata, autoload=True, autoload_with=db_engine)


def with_session(f):
    def with_session_func(*args, **kwargs):
        session = sessionmaker(bind=db_engine)()
        try:
            result = f(session, *args, **kwargs)
        except Exception:
            session.rollback()
            raise
        finally:
            session.commit()
        return result
    return with_session_func
