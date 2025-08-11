package com.scaleunlimited.flinksnippets.serialization;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.flink.api.common.functions.InvalidTypesException;
import org.apache.flink.api.common.typeinfo.TypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInfoFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.MapTypeInfo;

public class AccumulatorWithSet {

    @TypeInfo(SetTypeInfoFactory.class)
    private Set<String> customSet = new HashSet<>();
    
    public AccumulatorWithSet() { }

    public Set<String> getCustomSet() {
        return customSet;
    }

    public void setCustomSet(Set<String> customSet) {
        this.customSet = customSet;
    }
}
