from contextlib import contextmanager

from pandas.io import json
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


def connect(conn, connect_args={}):
    engine = create_engine(conn, json_serializer=json.dumps, connect_args=connect_args)
    return engine, sessionmaker(bind=engine)


@contextmanager
def session_scope(session_maker):
    """Provide a transactional scope around a series of operations."""
    session = session_maker()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
