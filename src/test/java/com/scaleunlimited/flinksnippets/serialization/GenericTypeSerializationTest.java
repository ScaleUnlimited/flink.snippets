package com.scaleunlimited.flinksnippets.serialization;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import java.lang.reflect.Type;

import org.apache.flink.api.common.functions.InvalidTypesException;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInfoFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.types.PojoTestUtils;
import org.apache.flink.types.PojoTestUtilsOld;
import org.junit.jupiter.api.Test;

class GenericTypeSerializationTest {

    public static class TestClass {
        private String name;
        private Integer data;

        public TestClass() {}
        
        public TestClass(String name, Integer data) {
            this.name = name;
            this.data = data;
        }
        
        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public Integer getData() {
            return data;
        }

        public void setData(Integer data) {
            this.data = data;
        }
    }
    
    public static class TestClassGeneric<T> {
        private String name;
        private T data;

        public TestClassGeneric() {}
        
        public TestClassGeneric(String name, T data) {
            this.name = name;
            this.data = data;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public T getData() {
            return data;
        }

        public void setData(T data) {
            this.data = data;
        }
    }
    
    public static class TestClassGenericList<T> {
        private String name;
        
        @TypeInfo(ListInfoFactory.class)
        private List<T> data;

        public TestClassGenericList() {}
        
        public TestClassGenericList(String name, T data) {
            this.name = name;
            this.data = new ArrayList<>();
            this.data.add(data);
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public List<T> getData() {
            return data;
        }

        public void setData(List<T> data) {
            this.data = data;
        }
    }
    
    @Test
    void test() throws Exception {
//        TestClass concrete = new TestClass("name", 0);
//        PojoTestUtils.assertSerializedAsPojoWithoutKryo(concrete.getClass());
        
        TestClassGenericList<Integer> genericList = new TestClassGenericList<>("name", 10);
        PojoTestUtils.assertSerializedAsPojo(genericList.getClass());

        TypeInformation<TestClassGenericList<Integer>> typeInfo = TypeInformation.of(new TypeHint<>(){});
        PojoTestUtilsOld.serialize(genericList, typeInfo);

//        TestClassGeneric<Integer> generic = new TestClassGeneric<>("name", 0);
//        PojoTestUtils.assertSerializedAsPojo(generic.getClass());
//        
//        TypeInformation<TestClassGeneric<Integer>> typeInfo = TypeInformation.of(new TypeHint<>(){});
//        PojoTestUtilsOld.serialize(generic, typeInfo);


    }
    
    public static class ListInfoFactory<E> extends TypeInfoFactory<List<E>> {
        @Override
        public TypeInformation<List<E>> createTypeInfo(Type t, Map<String, TypeInformation<?>> genericParameters) {
          TypeInformation<?> elementType = genericParameters.get("E");
          if (elementType == null) {
            throw new InvalidTypesException("Type extraction is not possible on List (element type unknown).");
          }

          return Types.LIST((TypeInformation<E>) elementType);
        }
      }


}
