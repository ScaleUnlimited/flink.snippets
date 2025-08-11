package com.scaleunlimited.flinksnippets.serialization;

import java.lang.reflect.Type;
import java.util.Map;
import java.util.Set;

import org.apache.flink.api.common.functions.InvalidTypesException;
import org.apache.flink.api.common.typeinfo.TypeInfoFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;

public class SetTypeInfoFactory<E> extends TypeInfoFactory<Set<E>> {

    @SuppressWarnings("unchecked")
    @Override
    public TypeInformation<Set<E>> createTypeInfo(
            Type t, Map<String, TypeInformation<?>> genericParameters) {
        TypeInformation<E> elementType = (TypeInformation<E>)genericParameters.get("E");

        if (elementType == null) {
            throw new InvalidTypesException(
                    "Type extraction is not possible on Set"
                            + " type as it does not contain information about the element type.");
        }

        return new SetTypeInfo<E>(elementType);
    }
}

