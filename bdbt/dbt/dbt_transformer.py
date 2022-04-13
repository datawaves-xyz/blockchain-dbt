from bdbt.abi.abi_data_type import EventSchema, CallSchema, ABIField
from bdbt.dbt.resource_type import DbtModel, DbtColumn


class DbtTransformer:
    @staticmethod
    def field_to_column(field: ABIField) -> DbtColumn:
        return DbtColumn(name=field.name)

    def event_schema_to_model(
            self,
            event: EventSchema,
            contract_name: str,
            event_name: str
    ) -> DbtModel:
        return DbtModel(
            name=f"{contract_name}_evt_{event_name}",
            columns=[self.field_to_column(i) for i in event.inputs]
        )

    def call_schema_to_model(
            self,
            call: CallSchema,
            contract_name: str,
            call_name: str,
            inputs: bool = True
    ) -> DbtModel:
        data = call.inputs if inputs else call.outputs

        return DbtModel(
            name=f"{contract_name}_call_{call_name}",
            columns=[self.field_to_column(i) for i in data]
        )
