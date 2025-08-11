package com.scaleunlimited.flinksnippets.examples;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;
import org.junit.Test;

public class TypeInformationTest {

    @Test
    public void testTypeInformation() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<List<String>> phoneTypeDS = env.addSource(new SourceFunction<List<String>>() {
            private Boolean isCancel = false;

            public void run(SourceFunction.SourceContext<List<String>> sourceContext) throws Exception {
                while (!isCancel) {
                    List<String> phoneTypeList = new ArrayList<String>();
                    phoneTypeList.add("iphone");
                    phoneTypeList.add("xiaomi");
                    phoneTypeList.add("meizu");

                    sourceContext.collect(phoneTypeList);
                    TimeUnit.SECONDS.sleep(3);
                }
            }

            public void cancel() {
                isCancel = true;
            }
        });

        phoneTypeDS
            .flatMap(new FlattenList())
            .print();

        env.execute("GenericTypeTest");
    }
    
    @SuppressWarnings("serial")
    private static class FlattenList implements FlatMapFunction<List<String>, String> {

        @Override
        public void flatMap(List<String> value, Collector<String> out) throws Exception {
            value.forEach(v -> out.collect(v));
        }
    }

}
