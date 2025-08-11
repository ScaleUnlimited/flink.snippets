package com.scaleunlimited.flinksnippets.serialization;

import java.util.HashSet;

import org.apache.flink.api.common.typeinfo.TypeInfo;

@TypeInfo(SetTypeInfoFactory.class)
public class StateAsSet<String> extends HashSet<String> {
}
