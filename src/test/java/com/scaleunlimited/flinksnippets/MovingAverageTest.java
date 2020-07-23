package com.scaleunlimited.flinksnippets;

import java.util.LinkedList;
import java.util.Queue;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;
import org.junit.Test;

public class MovingAverageTest {

    @Test
    public void testUsingPWF() throws Exception {
        LocalStreamEnvironment env = new LocalStreamEnvironment();
        env.setParallelism(2);

        DataStream<Tuple2<String, Long>> input = env.fromElements(
                new Tuple2<>("key1", 0L),
                new Tuple2<>("key1", 10L),
                new Tuple2<>("key1", 100L),
                new Tuple2<>("key1", 1L),
                new Tuple2<>("key2", 1L),
                new Tuple2<>("key2", 0L)
                );

        input
          .keyBy(new KeySelector<Tuple2<String,Long>, String>() {

            @Override
            public String getKey(Tuple2<String, Long> value) throws Exception {
                return value.f0;
            }
              
        })
          .window(GlobalWindows.create())
          .trigger(CountTrigger.of(1))
          .aggregate(new AverageAggregate(), new MyProcessWindowFunction())
          .print();
        
        env.execute();
    }
    
    @SuppressWarnings("serial")
    private static class AverageAggregate
            implements AggregateFunction<Tuple2<String, Long>, MovingAverageAccumulator, Double> {
        
        @Override
        public MovingAverageAccumulator createAccumulator() {
            return new MovingAverageAccumulator(2);
        }

        @Override
        public MovingAverageAccumulator add(Tuple2<String, Long> value, MovingAverageAccumulator acc) {
            acc.add(value.f1);
            return acc;
        }

        @Override
        public Double getResult(MovingAverageAccumulator acc) {
            return acc.getResult();
        }

        @Override
        public MovingAverageAccumulator merge(MovingAverageAccumulator a, MovingAverageAccumulator b) {
            a.merge(b);
            return a;
        }
    }

    @SuppressWarnings("serial")
    private static class MyProcessWindowFunction
            extends ProcessWindowFunction<Double, Tuple2<String, Double>, String, GlobalWindow> {

        @Override
        public void process(String key, Context context, Iterable<Double> averages,
                Collector<Tuple2<String, Double>> out) throws Exception {
            Double average = averages.iterator().next();
            out.collect(new Tuple2<>(key, average));
        }

    }
    
    private static class MovingAverageAccumulator {

        private int _numEntries;
        private Queue<Long> _values;
        
        public MovingAverageAccumulator(int numEntries) {
            _numEntries = numEntries;
            _values = new LinkedList<>();
        }

        public Queue<Long> getValues() {
            return _values;
        }
        
        public void add(long value) {
            _values.add(value);
            while (_values.size() > _numEntries) {
                _values.remove();
            }
        }

        public double getResult() {
            long total = 0L;
            for (long value : _values) {
                total += value;
            }
            
            return (double)total / _values.size();
        }

        public MovingAverageAccumulator merge(MovingAverageAccumulator acc2) {
            for (long value : acc2.getValues()) {
                add(value);
            }
            
            return this;
        }
    }
}
