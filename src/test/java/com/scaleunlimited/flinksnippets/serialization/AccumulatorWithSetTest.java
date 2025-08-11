package com.scaleunlimited.flinksnippets.serialization;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.types.PojoTestUtils;
import org.junit.jupiter.api.Test;

class AccumulatorWithSetTest {

    @Test
    void testSerializableWithoutKryo() {
        PojoTestUtils.assertSerializedAsPojoWithoutKryo(AccumulatorWithSet.class);
    }
    
    @Test
    void testSerialization() throws IOException {
        AccumulatorWithSet original = new AccumulatorWithSet();
        original.getCustomSet().add("one");
        original.getCustomSet().add("two");
        
        // Get TypeInformation for your custom class
        TypeInformation<AccumulatorWithSet> typeInfo = TypeInformation.of(AccumulatorWithSet.class);
        
        // Create serializer instance
        TypeSerializer<AccumulatorWithSet> serializer = typeInfo.createSerializer(new ExecutionConfig());
        
        DataOutputSerializer out = new DataOutputSerializer(1024);
        
        // Serialize the object
        serializer.serialize(original, out);
        
        // Create deserializer with the serialized bytes
        DataInputDeserializer in = new DataInputDeserializer(out.getCopyOfBuffer());
        
        // Deserialize and verify
        AccumulatorWithSet deserialized = serializer.deserialize(in);
        
        // Assert the objects are equal
        assertEquals(original.getCustomSet(), deserialized.getCustomSet());


    }

}
