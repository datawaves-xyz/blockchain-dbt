# blockchain-dbt

## Parse the function in the ABI to Call schema

```python
from bdbt.ethereum.abi.provider import SparkDataTypeProvider
from bdbt.ethereum.abi import ABITransformer
from bdbt.ethereum.abi.utils import normalize_abi

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
from bdbt.ethereum.dbt import DbtTransformer
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