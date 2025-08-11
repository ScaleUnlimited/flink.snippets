package com.scaleunlimited.flinksnippets.serialization;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.junit.jupiter.api.Test;

class StateAsSetTest {

    @Test
    void testSerialization() throws IOException {
        StateAsSet original = new StateAsSet();
        original.add("one");
        original.add("two");
        
        // Get TypeInformation for your custom class
        TypeInformation<StateAsSet> typeInfo = TypeInformation.of(new TypeHint<StateAsSet>() {
        });
        
        // Create serializer instance
        TypeSerializer<StateAsSet> serializer = typeInfo.createSerializer(new ExecutionConfig());
        
        DataOutputSerializer out = new DataOutputSerializer(1024);
        
        // Serialize the object
        serializer.serialize(original, out);
        
        // Create deserializer with the serialized bytes
        DataInputDeserializer in = new DataInputDeserializer(out.getCopyOfBuffer());
        
        // Deserialize and verify
        StateAsSet deserialized = serializer.deserialize(in);
        
        // Assert the objects are equal
        assertEquals(original, deserialized);
    }


}
