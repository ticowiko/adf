from uuid import uuid4

from typing import List, Type

from sqlalchemy.orm import DeclarativeMeta

from ADF.config import ADFGlobalConfig
from ADF.components.layer_transitions import ADLTransition
from ADF.components.data_structures import AbstractDataStructure, SQLDataStructure
from ADF.components.flow_config import ADFStep
from ADF.components.layers import AbstractDataLayer, SQLDataLayer
from ADF.exceptions import ADLUnsupportedTransition


class UniversalTransitionToSQL(ADLTransition):
    def default_to_write_out(self) -> bool:
        return True

    @staticmethod
    def get_handled_layers_in() -> List[Type[AbstractDataLayer]]:
        return [AbstractDataLayer]

    @staticmethod
    def get_handled_layers_out() -> List[Type[AbstractDataLayer]]:
        return [SQLDataLayer]

    def write_to_step(self, ads: AbstractDataStructure, step: ADFStep):
        self.layer_out: SQLDataLayer
        try:
            self.layer_out.session.bulk_save_objects(
                [
                    self.layer_out.get_step_table_meta(step)(**data)
                    for data in ads.to_list_of_dicts()
                ]
            )
        except TypeError as e:
            if "invalid keyword argument" in str(e):
                raise ValueError(
                    f"Got keyword argument type error when writing to step {str(step)} in {str(self)}, are you sure you've setup this transition ? Got : {str(e)}"
                )
            else:
                raise e
        self.layer_out.session.commit()

    def write_to_transition_step(
        self, ads: AbstractDataStructure, step: ADFStep, uuid: str
    ):
        self.layer_out: SQLDataLayer
        try:
            self.layer_out.session.bulk_save_objects(
                [
                    self.get_transition_step_table_meta(step)(
                        **row, **{ADFGlobalConfig.UUID_TAG_COLUMN_NAME: uuid}
                    )
                    for row in ads.to_list_of_dicts()
                ]
            )
        except TypeError as e:
            if "invalid keyword argument" in str(e):
                raise ValueError(
                    f"Got keyword argument type error when writing to transition step {str(step)} in {str(self)}, are you sure you've setup this transition ? Got : {str(e)}"
                )
            else:
                raise e
        self.layer_out.session.commit()

    def write_batch_data(
        self, ads: SQLDataStructure, step: ADFStep, batch_id: str
    ) -> None:
        self.layer_out: SQLDataLayer
        self.layer_out.attach_partition(ads, step)
        self.write_to_step(ads, step)

    def read_batch_data(self, step: ADFStep, batch_id: str) -> SQLDataStructure:
        self.layer_out: SQLDataLayer
        uuid = str(uuid4())
        self.write_to_transition_step(
            self.layer_in.read_batch_data(step, batch_id), step, uuid
        )
        ads = self.read_full_transition_data(step)
        return ads[ads[ADFGlobalConfig.UUID_TAG_COLUMN_NAME] == uuid]

    def read_full_data(self, step: ADFStep) -> SQLDataStructure:
        self.layer_out: SQLDataLayer
        uuid = str(uuid4())
        self.write_to_transition_step(self.layer_in.read_full_data(step), step, uuid)
        ads = self.read_full_transition_data(step)
        return ads[ads[ADFGlobalConfig.UUID_TAG_COLUMN_NAME] == uuid]

    def read_full_transition_data(self, step: ADFStep) -> SQLDataStructure:
        self.layer_out: SQLDataLayer
        table = self.get_transition_step_table_meta(step)
        subquery = self.layer_out.session.query(table).subquery()
        return SQLDataStructure(
            session=self.layer_out.session,
            subquery=subquery,
            cols={
                col_name: subquery.c[col_name]
                for col_name in [col.name for col in step.meta.columns]
                + [
                    ADFGlobalConfig.BATCH_ID_COLUMN_NAME,
                    ADFGlobalConfig.UUID_TAG_COLUMN_NAME,
                ]
            },
        )

    def get_transition_step_table_meta(self, step: ADFStep) -> DeclarativeMeta:
        self.layer_out: SQLDataLayer
        return self.layer_out.get_step_table_meta(
            step, extra_cols=[ADFGlobalConfig.UUID_TAG_COLUMN_NAME]
        )

    def setup_write_out(self, step_in: ADFStep, step_out: ADFStep) -> None:
        pass

    def setup_read_in(self, step_in: ADFStep, step_out: ADFStep) -> None:
        self.layer_out: SQLDataLayer
        self.get_transition_step_table_meta(step_in)
        self.layer_out.base.metadata.create_all(self.layer_out.session.get_bind())

    def delete_step_write_out(self, step: ADFStep):
        self.layer_out.delete_step(step)

    def delete_step_read_in(self, step: ADFStep):
        self.layer_out.delete_step(step)

    def delete_batch_write_out(self, step: ADFStep, batch_id: str):
        self.layer_out.delete_batch(step, batch_id)

    def delete_batch_read_in(self, step: ADFStep, batch_id: str):
        self.layer_out.delete_batch(step, batch_id)


class SameHostSQLToSQL(ADLTransition):
    def default_to_write_out(self) -> bool:
        return True

    def validate(self) -> None:
        super().validate()
        self.layer_in: SQLDataLayer
        self.layer_out: SQLDataLayer
        if (
            (self.layer_in.engine != self.layer_out.engine)
            or (self.layer_in.host != self.layer_out.host)
            or (self.layer_in.port != self.layer_out.port)
        ):
            raise ADLUnsupportedTransition(
                self,
                f"Mismatched hosts (IN : {self.layer_in.url}, OUT : {self.layer_out.url})",
            )

    @staticmethod
    def get_handled_layers_in() -> List[Type[AbstractDataLayer]]:
        return [SQLDataLayer]

    @staticmethod
    def get_handled_layers_out() -> List[Type[AbstractDataLayer]]:
        return [SQLDataLayer]

    def write_batch_data(
        self, ads: SQLDataStructure, step: ADFStep, batch_id: str
    ) -> None:
        self.layer_out.write_batch_data(ads, step, batch_id)

    def read_batch_data(self, step: ADFStep, batch_id: str) -> SQLDataStructure:
        self.layer_in: SQLDataLayer
        return self.layer_in.read_batch_data(step, batch_id)

    def read_full_data(self, step: ADFStep) -> SQLDataStructure:
        self.layer_in: SQLDataLayer
        return self.layer_in.read_full_data(step)

    def setup_write_out(self, step_in: ADFStep, step_out: ADFStep) -> None:
        pass

    def setup_read_in(self, step_in: ADFStep, step_out: ADFStep) -> None:
        pass

    def delete_step_write_out(self, step: ADFStep):
        self.layer_out.delete_step(step)

    def delete_step_read_in(self, step: ADFStep):
        self.layer_out.delete_step(step)

    def delete_batch_write_out(self, step: ADFStep, batch_id: str):
        self.layer_out.delete_batch(step, batch_id)

    def delete_batch_read_in(self, step: ADFStep, batch_id: str):
        self.layer_out.delete_batch(step, batch_id)
