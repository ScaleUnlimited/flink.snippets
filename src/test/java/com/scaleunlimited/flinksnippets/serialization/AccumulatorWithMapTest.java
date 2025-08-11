package com.scaleunlimited.flinksnippets.serialization;

import static org.junit.jupiter.api.Assertions.*;

import org.apache.flink.types.PojoTestUtils;
import org.junit.jupiter.api.Test;

class AccumulatorWithMapTest {

    @Test
    void testSerializableWithoutJava() {
        PojoTestUtils.assertSerializedAsPojoWithoutKryo(AccumulatorWithMap.class);
    }

}
