package io.iftech.sparkudf.hive;

import java.util.List;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.sparkproject.guava.collect.ImmutableList;

public class Opensea_WyvernExchangeV2_OrderApprovedPartOne_EventDecodeUDF extends DecodeContractEventHiveUDF {

    @Override
    public List<String> getInputDataFieldsName() {
        return ImmutableList.of("hash","exchange","maker","taker","makerRelayerFee","takerRelayerFee","makerProtocolFee","takerProtocolFee","feeRecipient","feeMethod","side","saleKind","target");
    }

    @Override
    public List<ObjectInspector> getInputDataFieldsOIs() {
        return ImmutableList.of(PrimitiveObjectInspectorFactory.writableBinaryObjectInspector,
PrimitiveObjectInspectorFactory.writableStringObjectInspector,
PrimitiveObjectInspectorFactory.writableStringObjectInspector,
PrimitiveObjectInspectorFactory.writableStringObjectInspector,
PrimitiveObjectInspectorFactory.writableStringObjectInspector,
PrimitiveObjectInspectorFactory.writableStringObjectInspector,
PrimitiveObjectInspectorFactory.writableStringObjectInspector,
PrimitiveObjectInspectorFactory.writableStringObjectInspector,
PrimitiveObjectInspectorFactory.writableStringObjectInspector,
PrimitiveObjectInspectorFactory.writableStringObjectInspector,
PrimitiveObjectInspectorFactory.writableStringObjectInspector,
PrimitiveObjectInspectorFactory.writableStringObjectInspector,
PrimitiveObjectInspectorFactory.writableStringObjectInspector);
    }
}
