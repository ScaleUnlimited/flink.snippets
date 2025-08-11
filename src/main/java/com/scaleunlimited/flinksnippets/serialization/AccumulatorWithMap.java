package com.scaleunlimited.flinksnippets.serialization;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;

import org.apache.flink.api.common.functions.InvalidTypesException;
import org.apache.flink.api.common.typeinfo.TypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInfoFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.MapTypeInfo;

public class AccumulatorWithMap {

    @TypeInfo(MapTypeInfoFactory.class)
    private Map<String, Long> customMap = new HashMap<>();
    
    public AccumulatorWithMap() { }

    public Map<String, Long> getCustomMap() {
        return customMap;
    }

    public void setCustomMap(Map<String, Long> customMap) {
        this.customMap = customMap;
    }
    
    public static class MapTypeInfoFactory<K, V> extends TypeInfoFactory<Map<K, V>> {

        @SuppressWarnings("unchecked")
        @Override
        public TypeInformation<Map<K, V>> createTypeInfo(
                Type t, Map<String, TypeInformation<?>> genericParameters) {
            TypeInformation<K> keyType = (TypeInformation<K>)genericParameters.get("K");
            TypeInformation<V> valueType = (TypeInformation<V>)genericParameters.get("V");

            if (keyType == null) {
                throw new InvalidTypesException(
                        "Type extraction is not possible on Map"
                                + " type as it does not contain information about the 'key' type.");
            }

            if (valueType == null) {
                throw new InvalidTypesException(
                        "Type extraction is not possible on Map"
                                + " type as it does not contain information about the 'value' type.");
            }
            
            return new MapTypeInfo<K, V>(keyType, valueType);
        }
    }

    
}
