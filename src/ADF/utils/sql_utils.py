import datetime

import psycopg2
from psycopg2.errors import DuplicateDatabase, DuplicateObject

from typing import Optional, Dict, Any, Union, Type, List

from sqlalchemy.orm import Session, DeclarativeMeta
from sqlalchemy import Column, String, Integer, Float, Boolean, DateTime, Date

from ADF.config import ADFGlobalConfig


def get_or_create(
    session: Session,
    model: DeclarativeMeta,
    defaults: Optional[Dict[str, Any]] = None,
    **kwargs,
):
    instance = session.query(model).filter_by(**kwargs).one_or_none()
    if instance:
        return instance, False
    else:
        params = kwargs.copy()
        params.update(defaults or {})
        instance = model(**params)
        session.add(instance)
        session.commit()
        return instance, True


def update_or_create(
    session: Session, model: Type[DeclarativeMeta], updates: Dict[str, Any], **kwargs
):
    instance = session.query(model).filter_by(**kwargs).one_or_none()
    if instance:
        for attr, val in updates.items():
            setattr(instance, attr, val)
        session.commit()
        return instance, False
    else:
        params = kwargs.copy()
        params.update(updates)
        instance = model(**params)
        session.add(instance)
        session.commit()
        return instance, True


def model2dict(instance):
    return {col.name: getattr(instance, col.name) for col in instance.__table__.columns}


sql_type_map = {
    str: String,
    int: Integer,
    float: Float,
    # complex: None,
    bool: Boolean,
    datetime.datetime: DateTime,
    datetime.date: Date,
    # datetime.timedelta: None,
    None: String,
}
sql_inverse_type_map = {
    val: key for key, val in sql_type_map.items() if key is not None
}


def create_table_meta(
    base: DeclarativeMeta,
    name: str,
    cols: Dict[
        str,
        Union[
            Type[str],
            Type[int],
            Type[float],
            Type[complex],
            Type[bool],
            Type[datetime.datetime],
            Type[datetime.date],
            Type[datetime.timedelta],
            Type[type(None)],
        ],
    ],
    primary_key: Optional[List[str]] = None,
    include_serial_pk: bool = True,
    include_timestamp_col: bool = True,
    include_batch_id_col: bool = True,
    **kwargs,
):
    primary_key = primary_key or []
    extra_cols = {}
    if include_serial_pk:
        extra_cols[ADFGlobalConfig.SQL_PK_COLUMN_NAME] = Column(
            Integer, primary_key=True, autoincrement=True
        )
    if include_timestamp_col:
        extra_cols[ADFGlobalConfig.TIMESTAMP_COLUMN_NAME] = Column(
            DateTime,
            nullable=False,
        )
    if include_batch_id_col:
        extra_cols[ADFGlobalConfig.BATCH_ID_COLUMN_NAME] = Column(
            String, nullable=False
        )
    return type(
        name,
        (base,),
        {
            "__tablename__": name,
            **extra_cols,
            **{
                col: Column(
                    sql_type_map[t], nullable=True, primary_key=col in primary_key
                )
                for col, t in cols.items()
            },
            **kwargs,
        },
    )


def setup_postgres_db(
    host: str,
    port: str,
    db: str,
    admin_user: str,
    admin_pw: str,
    users: List[Dict[str, str]],
):
    conn = psycopg2.connect(
        host=host,
        port=port,
        user=admin_user,
        password=admin_pw,
    )
    conn.autocommit = True
    cur = conn.cursor()
    try:
        cur.execute(f"CREATE database {db};")
    except DuplicateDatabase:
        pass
    for user in users:
        try:
            cur.execute(
                f"CREATE USER {user['user']} WITH ENCRYPTED PASSWORD '{user['pw']}';"
            )
        except DuplicateObject:
            cur.execute(
                f"ALTER USER {user['user']} WITH ENCRYPTED PASSWORD '{user['pw']}';"
            )
    cur.execute(
        f"GRANT ALL ON DATABASE {db} TO {', '.join([user['user'] for user in users])};"
    )
    conn.close()
