package com.scaleunlimited.flinksnippets;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.junit.Test;

import com.google.gson.JsonObject;

public class ProcessWindowFunctionTest {

    @Test
    public void test() throws Exception {
        LocalStreamEnvironment env = new LocalStreamEnvironment();
        env.setParallelism(2);

        long now = System.currentTimeMillis() - 1000;
        DataStream<Tuple2<String, JsonObject>> inputStream = env.fromElements(
                Tuple2.of("keyOne", makeJson("this", now)), 
                Tuple2.of("keyOne", makeJson(" and", now + 1_000L)), 
                Tuple2.of("keyOne", makeJson(" now", now + 2_000L)), 
                Tuple2.of("keyOne", makeJson(" that", now + 100_000L)), 
                Tuple2.of("keyThree", makeJson("different key", now)));

        DataStream<Tuple2<String, String>> agg = inputStream
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple2<String,JsonObject>>() {

                    @Override
                    public long extractAscendingTimestamp(Tuple2<String, JsonObject> element) {
                        return element.f1.get("time").getAsLong();
                    }
                })
                .keyBy(new KeySelector<Tuple2<String,JsonObject>, String>() {

                    @Override
                    public String getKey(Tuple2<String, JsonObject> in) throws Exception {
                        return in.f0;
                    }
                })
          .window(TumblingEventTimeWindows.of(Time.minutes(1)))
          .process(new MyProcessWindows());

        agg.print();
        
        env.execute();
    }
    
    private JsonObject makeJson(String key, long timestamp) {
        JsonObject result = new JsonObject();
        result.addProperty("value", key);
        result.addProperty("time", timestamp);
        return result;
    }

    @SuppressWarnings("serial")
    public class MyProcessWindows extends ProcessWindowFunction<Tuple2<String, JsonObject>, Tuple2<String, String>, String, TimeWindow> {

        @Override
        public void process(String key, Context context, Iterable<Tuple2<String, JsonObject>> in, Collector<Tuple2<String, String>> out)
                throws Exception {
            StringBuilder result = new StringBuilder();
            for (Tuple2<String, JsonObject> record : in) {
                result.append(record.f1.get("value").getAsString());
            }
            
            out.collect(new Tuple2<String, String>(key, result.toString()));
        }
        
    }

}
