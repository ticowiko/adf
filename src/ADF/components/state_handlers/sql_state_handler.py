import os
import datetime

from typing import Optional

from sqlalchemy import create_engine, Column, String, DateTime, Integer, Boolean
from sqlalchemy.orm import declarative_base, sessionmaker, Session

from ADF.components.flow_config import ADFStep
from ADF.components.state_handlers import AbstractStateHandler
from ADF.components.data_structures import SQLDataStructure
from ADF.utils import update_or_create


Base = declarative_base()


class BatchStatus(Base):
    __tablename__ = "batch_status"

    collection_name = Column(String, primary_key=True)
    flow_name = Column(String, primary_key=True)
    step_name = Column(String, primary_key=True)
    version = Column(String, primary_key=True)
    layer = Column(String, primary_key=True)
    batch_id = Column(String, primary_key=True)
    status = Column(String)
    datetime = Column(DateTime)
    msg = Column(String, nullable=True)


class BatchStatusTimeline(Base):
    __tablename__ = "batch_status_timeline"

    collection_name = Column(String, primary_key=True)
    flow_name = Column(String, primary_key=True)
    step_name = Column(String, primary_key=True)
    version = Column(String, primary_key=True)
    layer = Column(String, primary_key=True)
    batch_id = Column(String, primary_key=True)
    operation_count = Column(Integer, primary_key=True)
    latest = Column(Boolean)
    status = Column(String)
    datetime = Column(DateTime)
    msg = Column(String, nullable=True)


class SQLStateHandler(AbstractStateHandler):
    def __init__(self, url: Optional[str], **kwargs):
        super().__init__()
        self.url = url
        self.engine_args = kwargs
        self._session = None

    @property
    def session(self) -> Session:
        if self._session is None:
            if self.url is None:
                raise ValueError(
                    f"{self.__class__.__name__} cannot create session from null url (are you sure you've run setup ?)."
                )
            else:
                self._session = sessionmaker(
                    bind=create_engine(f"{self.url}", **self.engine_args)
                )()
        return self._session

    def setup(self):
        Base.metadata.create_all(self.session.get_bind())

    def destroy(self) -> None:
        if self.url is None:
            return
        if self.url.startswith("sqlite:///"):
            path = self.url.split(":///")[1]
            if os.path.isfile(path):
                os.remove(path)
        else:
            Base.metadata.drop_all(self.session.get_bind())

    def set_status(
        self, step: ADFStep, batch_id: str, status: str, msg: Optional[str] = None
    ) -> None:
        time = datetime.datetime.utcnow()
        update_or_create(
            self.session,
            BatchStatus,
            updates={"status": status, "datetime": time, "msg": msg},
            collection_name=step.flow.collection.name,
            flow_name=step.flow.name,
            step_name=step.name,
            version=step.version,
            layer=step.layer,
            batch_id=batch_id,
        )
        last = (
            self.session.query(BatchStatusTimeline)
            .filter_by(
                collection_name=step.flow.collection.name,
                flow_name=step.flow.name,
                step_name=step.name,
                version=step.version,
                layer=step.layer,
                batch_id=batch_id,
            )
            .order_by(BatchStatusTimeline.operation_count.desc())
            .first()
        )
        if last:
            operation_count = last.operation_count + 1
            last.latest = False
        else:
            operation_count = 0
        instance = BatchStatusTimeline(
            collection_name=step.flow.collection.name,
            flow_name=step.flow.name,
            step_name=step.name,
            version=step.version,
            layer=step.layer,
            batch_id=batch_id,
            operation_count=operation_count,
            latest=True,
            status=status,
            datetime=time,
            msg=msg,
        )
        self.session.add(instance)
        self.session.commit()

    def to_ads(self) -> SQLDataStructure:
        subquery = self.session.query(BatchStatus).subquery()
        return SQLDataStructure(
            session=self.session,
            subquery=subquery,
            cols={col.name: col for col in subquery.c},
        )

    def delete_entry(
        self,
        step: ADFStep,
        batch_id: str,
    ):
        self.session.query(BatchStatus).filter(
            BatchStatus.collection_name == step.flow.collection.name,
            BatchStatus.flow_name == step.flow.name,
            BatchStatus.step_name == step.name,
            BatchStatus.version == step.version,
            BatchStatus.batch_id == batch_id,
        ).delete()
        self.session.commit()
