package io.iftech.sparkudf.hive;

import java.util.List;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.sparkproject.guava.collect.ImmutableList;

public class Test_AllTypeFunctionCallDecodeUDF extends DecodeContractFunctionHiveUDF {

    @Override
    public List<String> getInputDataFieldsName() {
        return ImmutableList.of("addr","i8","i16","i32","i64","ui8","ui16","ui32","ui64","bool","f128x20","uf128x20","bytes","bytes10","str","int8_array","bytes12_array","bool_array","tuple");
    }

    @Override
    public List<ObjectInspector> getInputDataFieldsOIs() {
        return ImmutableList.of(PrimitiveObjectInspectorFactory.writableStringObjectInspector,
PrimitiveObjectInspectorFactory.writableIntObjectInspector,
PrimitiveObjectInspectorFactory.writableIntObjectInspector,
PrimitiveObjectInspectorFactory.writableIntObjectInspector,
PrimitiveObjectInspectorFactory.writableLongObjectInspector,
PrimitiveObjectInspectorFactory.writableIntObjectInspector,
PrimitiveObjectInspectorFactory.writableIntObjectInspector,
PrimitiveObjectInspectorFactory.writableLongObjectInspector,
PrimitiveObjectInspectorFactory.writableHiveDecimalObjectInspector,
PrimitiveObjectInspectorFactory.writableBooleanObjectInspector,
PrimitiveObjectInspectorFactory.writableHiveDecimalObjectInspector,
PrimitiveObjectInspectorFactory.writableHiveDecimalObjectInspector,
PrimitiveObjectInspectorFactory.writableBinaryObjectInspector,
PrimitiveObjectInspectorFactory.writableBinaryObjectInspector,
PrimitiveObjectInspectorFactory.writableStringObjectInspector,
ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableIntObjectInspector),
ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableBinaryObjectInspector),
ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableBooleanObjectInspector),
ObjectInspectorFactory.getStandardStructObjectInspector(
            ImmutableList.of("value","key"),
            ImmutableList.of(PrimitiveObjectInspectorFactory.writableHiveDecimalObjectInspector,
PrimitiveObjectInspectorFactory.writableStringObjectInspector)
        )
        );
    }

    @Override
    public List<String> getOutputDataFieldsName() {
        return ImmutableList.of("addrs","uints","feeMethod","side","saleKind","howToCall","calldata","replacementPattern","staticExtradata");
    }

    @Override
    public List<ObjectInspector> getOutputDataFieldsOIs() {
        return ImmutableList.of(ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableStringObjectInspector),
ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableHiveDecimalObjectInspector),
PrimitiveObjectInspectorFactory.writableIntObjectInspector,
PrimitiveObjectInspectorFactory.writableIntObjectInspector,
PrimitiveObjectInspectorFactory.writableIntObjectInspector,
PrimitiveObjectInspectorFactory.writableIntObjectInspector,
PrimitiveObjectInspectorFactory.writableBinaryObjectInspector,
PrimitiveObjectInspectorFactory.writableBinaryObjectInspector,
PrimitiveObjectInspectorFactory.writableBinaryObjectInspector);
    }
}