import os.path
import subprocess

from bdbt.abi.abi_data_type import ABIEventSchema, ABICallSchema
from bdbt.dbt.dbt_code_generator import DbtCodeGenerator
from bdbt.provider.hive_object_inspector_type_provider import HiveObjectInspectorTypeProvider

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
}"""

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
}"""


class SparkDbtUDFGenerator(DbtCodeGenerator):
    hive_provider = HiveObjectInspectorTypeProvider()

    def __init__(self):
        super(SparkDbtUDFGenerator, self).__init__(True)

    def generate_event_dbt_model(self, project_path: str, contract_name: str, event: ABIEventSchema):
        pass

    def generate_call_dbt_model(self, project_path: str, contract_name: str, call: ABICallSchema):
        pass

    def generate_event_udf(self, udf_workspace: str, contract_name: str, event: ABIEventSchema):
        clazz_name = contract_name + '_' + event['name'] + 'EventDecodeUDF'
        filepath = os.path.join(udf_workspace, clazz_name + '.java')

        field_names = ','.join([f'"{i.name}"' for i in event['inputs']])
        # TODO: java file indent style
        field_ois = ',\n'.join([self.hive_provider.transform(i.ftype) for i in event['inputs']])
        content = event_clazz_template \
            .replace('{{CLASS_NAME}}', clazz_name) \
            .replace('{{FIELD_NAMES}}', field_names) \
            .replace('{{OBJECT_INSPECTORS}}', field_ois)

        self.create_file_and_write(filepath, content)

    def generate_call_udf(self, udf_workspace: str, contract_name: str, call: ABICallSchema):
        clazz_name = contract_name + '_' + call['name'] + 'CallDecodeUDF'
        filepath = os.path.join(udf_workspace, clazz_name + '.java')

        input_field_names = ','.join([f'"{i.name}"' for i in call['inputs']])
        input_field_ois = ',\n'.join([self.hive_provider.transform(i.ftype) for i in call['inputs']])

        output_field_names = ','.join([f'"{i.name}"' for i in call['outputs']])
        output_field_ois = ',\n'.join([self.hive_provider.transform(i.ftype) for i in call['outputs']])
        content = call_clazz_template \
            .replace('{{CLASS_NAME}}', clazz_name) \
            .replace('{{INPUT_FIELD_NAMES}}', input_field_names) \
            .replace('{{INPUT_OBJECT_INSPECTORS}}', input_field_ois) \
            .replace('{{OUTPUT_FIELD_NAMES}}', output_field_names) \
            .replace('{{OUTPUT_OBJECT_INSPECTORS}}', output_field_ois)

        self.create_file_and_write(filepath, content)

    def build_udf(self, project_path: str):
        java_project_path = os.path.join(project_path, 'java')
        jar_path = os.path.join(project_path, 'udf.jar')

        # TODO: use more flexable filename
        self._execute_command(
            f"""
            cd {java_project_path} \
            && mvn clean package -DskipTests \
            && mv target/blockchain-spark-0.5.0-jar-with-dependencies.jar {jar_path} \
            && cd {project_path} \
            && rm -rf java
            """
        )

    def prepare_udf_workspace(self, project_path: str) -> str:
        # clone blockchain-spark project and move java dir to the root
        # TODO: release blockchain-spark project to maven central
        # TODO: use master branch if the pr is merged
        self._execute_command(
            f"""
            cd {project_path}  \
            && git clone https://github.com/datawaves-xyz/blockchain-spark.git \
            && cd blockchain-spark \
            && git checkout hive-udf \
            && cd {project_path} \
            && mv blockchain-spark/java java \
            && rm -rf blockchain-spark
            """
        )
        return os.path.join(project_path, 'java/src/main/java/io/iftech/sparkudf/hive')

    def _execute_command(self, command: str):
        p = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = p.communicate()

        if out:
            self.logger.info("standard output of subprocess:")
            self.logger.info(out.decode('utf-8'))

        if p.returncode != 0:
            raise ChildProcessError(f'execute {command} getting error response: {err.decode("utf-8")}')
