/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.flink.types;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.api.java.typeutils.runtime.PojoSerializer;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;

/** Test utils around POJOs. */
public class PojoTestUtilsOld {
    /**
     * Verifies that instances of the given class fulfill all conditions to be serialized with the
     * {@link PojoSerializer}, as documented <a href=
     * "https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/fault-tolerance/serialization/types_serialization/#pojos">here</a>.
     *
     * @param clazz class to analyze
     * @param <T> class type
     * @throws AssertionError if instances of the class cannot be serialized as a POJO
     */
    public static <T> void assertSerializedAsPojo(Class<T> clazz) throws AssertionError {
        final TypeInformation<T> typeInformation = TypeInformation.of(clazz);
        final TypeSerializer<T> actualSerializer =
                typeInformation.createSerializer(new ExecutionConfig());

        String failMsg =
                String.format(
                        "Instances of the class '%s' cannot be serialized as a POJO, but would use a '%s' instead.\n"
                                + "Re-run this test with INFO logging enabled and check messages from the '%s' for possible reasons.",
                        clazz.getSimpleName(),
                        actualSerializer.getClass().getSimpleName(),
                        TypeExtractor.class.getCanonicalName());
        assertTrue(actualSerializer instanceof PojoSerializer, failMsg);
    }
    
    public static <T> byte[] serializeWithoutKryo(T record, TypeInformation<T> typeInfo) throws IOException {
        ExecutionConfig config = new ExecutionConfig();
        config.disableGenericTypes();
        return serialize(record, typeInfo, config);
    }
    
    public static <T> byte[] serialize(T record, TypeInformation<T> typeInfo) throws IOException {
        return serialize(record, typeInfo, new ExecutionConfig());
    }
    
    private static <T> byte[] serialize(T record, TypeInformation<T> typeInfo, ExecutionConfig config) throws IOException {
        final TypeSerializer<T> actualSerializer = typeInfo.createSerializer(config);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputView dov = new DataOutputViewStreamWrapper(baos);
        actualSerializer.serialize(record, dov);
        
        return baos.toByteArray();
    }
}
