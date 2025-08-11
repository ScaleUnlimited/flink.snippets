package com.scaleunlimited.flinksnippets.partitioning;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSink;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.Test;


class WMAwareProcessOperatorTest {

    @Test
    public void test() throws Exception {
        System.setProperty("my.root.level", "INFO");
        
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(2);
                
        TypeInformation<Tuple2<String, Integer>> outputType = TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() { });
        WMAwareProcessOperator<String, Tuple2<String, Integer>> operator = new WMAwareProcessOperator<>(new CountryCountAggregator());
        
        env.addSource(new SkewedCountrySource(), "source")
            .partitionCustom(new SkewedCountryPartitioner(), r -> r)
            .transform("Partioned aggregator", outputType, operator)
            .sinkTo(new PrintSink<>());
        
        env.execute();
    }
    
    // List of countries that we're counting
    //
    private static final String[] COUNTRIES = new String[] {
            "US",
            "UK",
            "CA",
            "JP",
            "FR",
            "DE",
            "AS",
            "AU",
            "NZ",
            "SW"
    };
    

    /**
     * Source function that generates skewed data
     *
     */
    @SuppressWarnings("serial")
    private static class SkewedCountrySource extends RichParallelSourceFunction<String> {

        private boolean _keepRunning = true;
        private transient Random rand;
        
        
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            
            rand = new Random(666L);
        }
        
        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            int count = 0;
            
            while (_keepRunning && (count++ < 1000)) {
                int countryIndex;
                // 50% of all data is our first country.
                if (rand.nextInt(2) == 0) {
                    countryIndex = 0;
                } else {
                    countryIndex = 1 + rand.nextInt(COUNTRIES.length - 1);
                }
                
                ctx.collect(COUNTRIES[countryIndex]);
            }
        }

        @Override
        public void cancel() {
            _keepRunning = false;
        }
    }
    
    /**
     * Partitioner that assigns the skewed country (item 0 in array) to its own
     * slot.
     *
     */
    @SuppressWarnings("serial")
    private static class SkewedCountryPartitioner implements Partitioner<String> {

        @Override
        public int partition(String country, int parallelism) {
            // If it's our skewed country, always use slot 0, otherwise distribute
            // randomly among remaining slots.
            if (country.equals(COUNTRIES[0])) {
                return 0;
            } else {
                return 1 + (Math.abs(country.hashCode()) % (parallelism - 1));
            }
        }
    }
    
    /**
     * A ProcessFunction that gets called when watermarks arrive, so it knows when to generate
     * final results.
     *
     */
    @SuppressWarnings("serial")
    private static class CountryCountAggregator extends WMAwareProcessFunction<String, Tuple2<String, Integer>> {

        private transient Map<String, Integer> countryCounts;
        
        @Override
        public void open(Configuration parameters) throws Exception {
            countryCounts = new HashMap<>();
        }
        
        @Override
        public void processElement(String in, Context ctx, Collector<Tuple2<String, Integer>> out)
                throws Exception {
            Integer curCount = countryCounts.getOrDefault(in, 0);
            countryCounts.put(in, curCount + 1);
        }
        
        // We get MAX_WATERMARK when the source is done. Note that the StreamRecord we generate has a timestamp of 0, since
        // we don’t care about event time when generating bounded (batch) results.
        public void processWatermark(Watermark watermark, Collector<StreamRecord<Tuple2<String, Integer>>> out) {
            if (watermark.getTimestamp() == Watermark.MAX_WATERMARK.getTimestamp()) {
                for (String country : countryCounts.keySet()) {
                    out.collect(new StreamRecord<Tuple2<String,Integer>>(Tuple2.of(country, countryCounts.get(country)), 0));
                }
            }
            
        }
        
    }

}
