from typing import Generic, TypeVar

from bdbt.abi.abi_data_type import EventSchema, CallSchema, ABIField
from bdbt.dbt.resource_type import DbtModel, DbtColumn, DbtMeta

T = TypeVar('T')


class DbtTransformer:
    def __init__(self, provider: Generic[T]):
        self.provider = provider

    def field_to_column(self, field: ABIField[T]) -> DbtColumn:
        return DbtColumn(
            name=field.name,
            meta=DbtMeta(type=self.provider.get_type_name(field.ftype))
        )

    def event_schema_to_model(
            self,
            event: EventSchema[T],
            contract_name: str,
            event_name: str
    ) -> DbtModel:
        return DbtModel(
            name=f"{contract_name}_evt_{event_name}",
            columns=[self.field_to_column(i) for i in event.inputs]
        )

    def call_schema_to_model(
            self,
            call: CallSchema[T],
            contract_name: str,
            call_name: str,
            inputs: bool = True
    ) -> DbtModel:
        data = call.inputs if inputs else call.outputs

        return DbtModel(
            name=f"{contract_name}_call_{call_name}",
            columns=[self.field_to_column(i) for i in data]
        )
