# blockchain-dbt

## Parse the function in the ABI to Call schema

```python
from bdbt.provider.spark_type_provider import SparkDataTypeProvider
from bdbt.abi.abi_transformer import ABITransformer
from bdbt.abi.utils import normalize_abi

provider = SparkDataTypeProvider()
transformer = ABITransformer(provider)
abi = normalize_abi(abi_json_str)
abi_call_schema = transformer.transform_abi_call(abi=abi, call_name='atomicMatch')
```

## ABI Call schema to database schema

```python
call_schema = transformer.transform_to_call_schema(abi_call_schema)
```

## Database schema to dbt model

```python
from bdbt.dbt.dbt_transformer import DbtTransformer
import pyaml

dbt_transformer = DbtTransformer(provider=provider)
model = dbt_transformer.call_schema_to_model(
    call=call_schema,
    contract_name="WyvernExchange",
    call_name="atomicMatch"
)

print(pyaml.dump(model))
```

```yaml
name: WyvernExchange_call_atomicMatch
columns:
  - meta:
      type: string
    name: addr
  ...
```