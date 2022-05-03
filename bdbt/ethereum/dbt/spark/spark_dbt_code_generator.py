import json
import os.path
import pathlib
import subprocess
import xml.etree.ElementTree as ET

from eth_utils import event_abi_to_log_topic, encode_hex, function_abi_to_4byte_selector

from bdbt.content import Contract
from bdbt.ethereum.abi.abi_data_type import ABIEventSchema, ABICallSchema
from bdbt.ethereum.abi.provider.hive_object_inspector_type_provider import HiveObjectInspectorTypeProvider
from bdbt.ethereum.dbt.dbt_code_generator import DbtCodeGenerator

event_clazz_template = """package io.iftech.sparkudf.hive;

import java.util.List;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.sparkproject.guava.collect.ImmutableList;

public class {{CLASS_NAME}} extends DecodeContractEventHiveUDF {

    @Override
    public List<String> getInputDataFieldsName() {
        return ImmutableList.of({{FIELD_NAMES}});
    }

    @Override
    public List<ObjectInspector> getInputDataFieldsOIs() {
        return ImmutableList.of({{OBJECT_INSPECTORS}});
    }
}
"""

call_clazz_template = """package io.iftech.sparkudf.hive;

import java.util.List;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.sparkproject.guava.collect.ImmutableList;

public class {{CLASS_NAME}} extends DecodeContractFunctionHiveUDF {

    @Override
    public List<String> getInputDataFieldsName() {
        return ImmutableList.of({{INPUT_FIELD_NAMES}});
    }

    @Override
    public List<ObjectInspector> getInputDataFieldsOIs() {
        return ImmutableList.of({{INPUT_OBJECT_INSPECTORS}});
    }

    @Override
    public List<String> getOutputDataFieldsName() {
        return ImmutableList.of({{OUTPUT_FIELD_NAMES}});
    }

    @Override
    public List<ObjectInspector> getOutputDataFieldsOIs() {
        return ImmutableList.of({{OUTPUT_OBJECT_INSPECTORS}});
    }
}
"""

empty_event_dbt_model_sql_template = """{{
    config(
        {{MODEL_MATERIALIZED_CONFIG}},
        file_format='parquet',
        alias='{{MODEL_ALIAS}}'
    )
}}

select /* REPARTITION({{MODEL_REPARTITION_COUNT}}) */
    block_number as evt_block_number,
    block_timestamp as evt_block_time,
    log_index as evt_index,
    transaction_hash as evt_tx_hash,
    address as contract_address,
    dt
from {{ ref('stg_logs') }}
where address = lower("{{CONTRACT_ADDRESS}}")
and address_hash = abs(hash(lower("{{CONTRACT_ADDRESS}}"))) % 10
and selector = "{{EVENT_SELECTOR}}"
and selector_hash = abs(hash("{{EVENT_SELECTOR}}")) % 10

{% if is_incremental() %}
  and dt = '{{ var("dt") }}'
{% endif %}
"""

event_dbt_model_sql_template = """{{
    config(
        {{MODEL_MATERIALIZED_CONFIG}},
        file_format='parquet',
        alias='{{MODEL_ALIAS}}',
        pre_hook={
            'sql': 'create or replace function {{UDF_NAME}} as "io.iftech.sparkudf.hive.{{CLASS_NAME}}" using jar "{{UDF_JAR_PATH}}";'
        }
    )
}}

with base as (
    select
        block_number as evt_block_number,
        block_timestamp as evt_block_time,
        log_index as evt_index,
        transaction_hash as evt_tx_hash,
        address as contract_address,
        dt,
        {{UDF_NAME}}(unhex_data, topics_arr, '{{EVENT_ABI}}', '{{EVENT_NAME}}') as data
    from {{ ref('stg_logs') }}
    where address = lower("{{CONTRACT_ADDRESS}}")
    and address_hash = abs(hash(lower("{{CONTRACT_ADDRESS}}"))) % 10
    and selector = "{{EVENT_SELECTOR}}"
    and selector_hash = abs(hash("{{EVENT_SELECTOR}}")) % 10

    {% if is_incremental() %}
      and dt = '{{ var("dt") }}'
    {% endif %}
),

final as (
    select
        evt_block_number,
        evt_block_time,
        evt_index,
        evt_tx_hash,
        contract_address,
        dt,
        data.input.*
    from base
)

select /* REPARTITION({{MODEL_REPARTITION_COUNT}}) */ *
from final
"""

empty_call_dbt_model_sql_template = """{{
    config(
        {{MODEL_MATERIALIZED_CONFIG}},
        file_format='parquet',
        alias='{{MODEL_ALIAS}}'
    )
}}

select /* REPARTITION({{MODEL_REPARTITION_COUNT}}) */
    status==1 as call_success,
    block_number as call_block_number,
    block_timestamp as call_block_time,
    trace_address as call_trace_address,
    transaction_hash as call_tx_hash,
    to_address as contract_address,
    dt
from {{ ref('stg_traces') }}
where to_address = lower("{{CONTRACT_ADDRESS}}")
and address_hash = abs(hash(lower("{{CONTRACT_ADDRESS}}"))) % 10
and selector = "{{CALL_SELECTOR}}"
and selector_hash = abs(hash("{{CALL_SELECTOR}}")) % 10

{% if is_incremental() %}
  and dt = '{{ var("dt") }}'
{% endif %}
"""

call_dbt_model_sql_template = """{{
    config(
        {{MODEL_MATERIALIZED_CONFIG}},
        file_format='parquet',
        alias='{{MODEL_ALIAS}}',
        pre_hook={
            'sql': 'create or replace function {{UDF_NAME}} as "io.iftech.sparkudf.hive.{{CLASS_NAME}}" using jar "{{UDF_JAR_PATH}}";'
        }
    )
}}

with base as (
    select
        status==1 as call_success,
        block_number as call_block_number,
        block_timestamp as call_block_time,
        trace_address as call_trace_address,
        transaction_hash as call_tx_hash,
        to_address as contract_address,
        dt,
        {{UDF_NAME}}(unhex_input, unhex_output, '{{CALL_ABI}}', '{{CALL_NAME}}') as data
    from {{ ref('stg_traces') }}
    where to_address = lower("{{CONTRACT_ADDRESS}}")
    and address_hash = abs(hash(lower("{{CONTRACT_ADDRESS}}"))) % 10
    and selector = "{{CALL_SELECTOR}}"
    and selector_hash = abs(hash("{{CALL_SELECTOR}}")) % 10

    {% if is_incremental() %}
      and dt = '{{ var("dt") }}'
    {% endif %}
),

final as (
    select
        call_success,
        call_block_number,
        call_block_time,
        call_trace_address,
        call_tx_hash,
        contract_address,
        dt,
        data.input.*,
        data.output.*
    from base
)

select /* REPARTITION({{MODEL_REPARTITION_COUNT}}) */ *
from final
"""

table_model_config = "materialized='table'"
increment_model_config = "materialized='incremental', incremental_strategy='insert_overwrite', partition_by=['dt']"


class SparkDbtCodeGenerator(DbtCodeGenerator):
    hive_provider = HiveObjectInspectorTypeProvider()

    def __init__(self, remote_workspace: str):
        super(SparkDbtCodeGenerator, self).__init__(True)
        self.remote_workspace = remote_workspace

    def gen_event_dbt_model(
            self,
            project_path: str,
            contract: Contract,
            version: str,
            event: ABIEventSchema
    ) -> None:
        contract_name = contract['name']
        contract_address = contract['address']
        contract_materialize = contract['materialize']

        project_name = pathlib.Path(project_path).name
        filepath = os.path.join(project_path, self.evt_model_name(contract_name, event, project_name) + '.sql')
        event_selector = encode_hex(event_abi_to_log_topic(event.raw_schema))

        if event.is_empty:
            content = event_dbt_model_sql_template \
                .replace('{{CONTRACT_ADDRESS}}', contract_address) \
                .replace('{{EVENT_SELECTOR}}', event_selector) \
                .replace('{{MODEL_ALIAS}}', self.evt_model_name(contract_name, event).lower()) \
                .replace('{{MODEL_MATERIALIZED_CONFIG}}', self._materialized_config(contract_materialize)) \
                .replace('{{MODEL_REPARTITION_COUNT}}', self._repartition_count(contract_materialize))
        else:
            clazz_name = self._event_udf_class_name(project_name, contract_name, event)
            content = event_dbt_model_sql_template \
                .replace('{{UDF_NAME}}', clazz_name.lower()) \
                .replace('{{CLASS_NAME}}', clazz_name) \
                .replace('{{UDF_JAR_PATH}}', os.path.join(self.remote_workspace, self._jar_name(version))) \
                .replace('{{EVENT_ABI}}', json.dumps(event.raw_schema)) \
                .replace('{{EVENT_NAME}}', event.name) \
                .replace('{{CONTRACT_ADDRESS}}', contract_address) \
                .replace('{{EVENT_SELECTOR}}', event_selector) \
                .replace('{{MODEL_ALIAS}}', self.evt_model_name(contract_name, event).lower()) \
                .replace('{{MODEL_MATERIALIZED_CONFIG}}', self._materialized_config(contract_materialize)) \
                .replace('{{MODEL_REPARTITION_COUNT}}', self._repartition_count(contract_materialize))

        self.create_file_and_write(filepath, content)

    def gen_call_dbt_model(
            self,
            project_path: str,
            contract: Contract,
            version: str,
            call: ABICallSchema
    ) -> None:
        contract_name = contract['name']
        contract_address = contract['address']
        contract_materialize = contract['materialize']

        project_name = pathlib.Path(project_path).name
        filepath = os.path.join(project_path, self.call_model_name(contract_name, call, project_name) + '.sql')
        call_selector = encode_hex(function_abi_to_4byte_selector(call.raw_schema))

        if call.is_empty:
            content = empty_call_dbt_model_sql_template \
                .replace('{{CONTRACT_ADDRESS}}', contract_address) \
                .replace('{{CALL_SELECTOR}}', call_selector) \
                .replace('{{MODEL_ALIAS}}', self.call_model_name(contract_name, call).lower()) \
                .replace('{{MODEL_MATERIALIZED_CONFIG}}', self._materialized_config(contract_materialize)) \
                .replace('{{MODEL_REPARTITION_COUNT}}', self._repartition_count(contract_materialize))
        else:
            clazz_name = self._call_udf_class_name(project_name, contract_name, call)
            content = call_dbt_model_sql_template \
                .replace('{{UDF_NAME}}', clazz_name.lower()) \
                .replace('{{CLASS_NAME}}', clazz_name) \
                .replace('{{UDF_JAR_PATH}}', os.path.join(self.remote_workspace, self._jar_name(version))) \
                .replace('{{CALL_ABI}}', json.dumps(call.raw_schema)) \
                .replace('{{CALL_NAME}}', call.name) \
                .replace('{{CONTRACT_ADDRESS}}', contract_address) \
                .replace('{{CALL_SELECTOR}}', call_selector[0:10]) \
                .replace('{{MODEL_ALIAS}}', self.call_model_name(contract_name, call).lower()) \
                .replace('{{MODEL_MATERIALIZED_CONFIG}}', self._materialized_config(contract_materialize)) \
                .replace('{{MODEL_REPARTITION_COUNT}}', self._repartition_count(contract_materialize))

        self.create_file_and_write(filepath, content)

    def gen_event_udf(
            self, udf_workspace: str, project_name: str, contract_name: str, event: ABIEventSchema
    ) -> None:
        clazz_name = self._event_udf_class_name(project_name, contract_name, event)
        filepath = os.path.join(udf_workspace, clazz_name + '.java')

        field_names = ','.join([f'"{i.name}"' for i in event.inputs])
        # TODO: java file indent style
        field_ois = ',\n'.join([self.hive_provider.transform(i.ftype) for i in event.inputs])
        content = event_clazz_template \
            .replace('{{CLASS_NAME}}', clazz_name) \
            .replace('{{FIELD_NAMES}}', field_names) \
            .replace('{{OBJECT_INSPECTORS}}', field_ois)

        self.create_file_and_write(filepath, content)

    def gen_call_udf(
            self, udf_workspace: str, project_name: str, contract_name: str, call: ABICallSchema
    ) -> None:
        clazz_name = self._call_udf_class_name(project_name, contract_name, call)
        filepath = os.path.join(udf_workspace, clazz_name + '.java')

        input_field_names = ','.join([f'"{i.name}"' for i in call.inputs])
        input_field_ois = ',\n'.join([self.hive_provider.transform(i.ftype) for i in call.inputs])

        output_field_names = ','.join([f'"{i.name}"' for i in call.outputs])
        output_field_ois = ',\n'.join([self.hive_provider.transform(i.ftype) for i in call.outputs])
        content = call_clazz_template \
            .replace('{{CLASS_NAME}}', clazz_name) \
            .replace('{{INPUT_FIELD_NAMES}}', input_field_names) \
            .replace('{{INPUT_OBJECT_INSPECTORS}}', input_field_ois) \
            .replace('{{OUTPUT_FIELD_NAMES}}', output_field_names) \
            .replace('{{OUTPUT_OBJECT_INSPECTORS}}', output_field_ois)

        self.create_file_and_write(filepath, content)

    def build_udf(
            self, dbt_dir: str, version: str
    ) -> None:
        java_project_path = os.path.join(dbt_dir, 'java')

        root = ET.parse(os.path.join(java_project_path, 'pom.xml')).getroot()
        blockchain_spark_version = root.find('{http://maven.apache.org/POM/4.0.0}version').text

        # The version of the jar package should be the same with the version of the dbt project.
        jar_path = os.path.join(dbt_dir, self._jar_name(version))

        self._execute_command(
            command=f"""
            mvn clean package -DskipTests \
            && mv target/blockchain-spark-{blockchain_spark_version}-jar-with-dependencies.jar {jar_path} \
            && cd {dbt_dir} \
            && rm -rf java
            """,
            dir=java_project_path
        )

    def prepare_udf_workspace(self, dbt_dir: str) -> str:
        # clone blockchain-spark project and move java dir to the root
        # TODO: release blockchain-spark project to maven central
        self._execute_command(
            command=f"""
            git clone https://github.com/datawaves-xyz/blockchain-spark.git \
            && mv blockchain-spark/java java \
            && rm -rf blockchain-spark
            """,
            dir=dbt_dir
        )
        return os.path.join(dbt_dir, 'java/src/main/java/io/iftech/sparkudf/hive')

    def _execute_command(self, command: str, dir: str):
        sp = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            cwd=dir,
            shell=True,
            close_fds=True)
        self.logger.info("Output:")
        for line in iter(sp.stdout.readline, b''):
            line = line.decode('utf-8').rstrip()
            self.logger.info(line)
        sp.wait()
        self.logger.info(
            "Command exited with return code %s",
            sp.returncode
        )

        if sp.returncode != 0:
            err_msg = '\n'.join([line.decode('utf-8').rstrip() for line in iter(sp.stderr.readline, b'')])
            raise ChildProcessError(f'execute {command} getting error response: {err_msg}')

    @staticmethod
    def _event_udf_class_name(
            project_name: str, contract_name: str, event: ABIEventSchema
    ) -> str:
        return project_name[0].upper() + project_name[1:] \
               + '_' + contract_name + '_' + event.name + '_' + 'EventDecodeUDF'

    @staticmethod
    def _call_udf_class_name(
            project_name: str, contract_name: str, call: ABICallSchema
    ) -> str:
        return project_name[0].upper() + project_name[1:] \
               + '_' + contract_name + '_' + call.name + '_' + 'CallDecodeUDF'

    @staticmethod
    def _jar_name(version: str) -> str:
        return f'blockchain-dbt-udf-{version}.jar'

    @staticmethod
    def _materialized_config(materialize: str) -> str:
        if materialize == 'table':
            return table_model_config
        elif materialize == 'increment':
            return increment_model_config
        else:
            raise ValueError(f'{materialize} isnt a supported materialized model.')

    @staticmethod
    def _repartition_count(materialize: str) -> str:
        if materialize == 'table':
            return '1'
        elif materialize == 'increment':
            return 'dt'
        else:
            raise ValueError(f'{materialize} isnt a supported materialized model.')
