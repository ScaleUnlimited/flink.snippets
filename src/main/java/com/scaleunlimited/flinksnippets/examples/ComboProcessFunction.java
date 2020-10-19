package com.scaleunlimited.flinksnippets.examples;

import java.util.List;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * Process an incoming stream of <I> records, keyed by <K>, using two provided process
 * functions, and output results to two side outputs.
 *
 * @param <K>
 * @param <I>
 * @param <O1>
 * @param <O2>
 */
@SuppressWarnings("serial")
public class ComboProcessFunction<K, I, O1, O2> extends KeyedProcessFunction<K, I, Void> {

    private KeyedProcessFunction<K, I, O1> processA;
    private KeyedProcessFunction<K, I, O2> processB;
    private OutputTag<O1> output1;
    private OutputTag<O2> output2;
    
    private transient Collector<O1> collector1;
    private transient List<O1> results1;
    private transient Collector<O2> collector2;
    private transient List<O2> results2;

    public ComboProcessFunction(KeyedProcessFunction<K, I, O1> processA, TypeInformation<O1> typeInfoA,
            KeyedProcessFunction<K, I, O2> processB, TypeInformation<O2> typeInfoB) {
        this.processA = processA;
        this.processB = processB;
        
        output1 = new OutputTag<>("processA", typeInfoA);
        output2 = new OutputTag<>("processB", typeInfoB);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        
        processA.open(parameters);
        processB.open(parameters);
        
        collector1 = new Collector<O1>() {

            @Override
            public void collect(O1 record) { results1.add(record); }

            @Override
            public void close() { }
        };
        
        collector2 = new Collector<O2>() {

            @Override
            public void collect(O2 record) { results2.add(record); }

            @Override
            public void close() { }
        };
    }
    
    @Override
    public void processElement(I in, Context ctx, Collector<Void> out) throws Exception {
        processA.processElement(in, null, collector1);
        for (O1 result1 : results1) {
            ctx.output(output1, result1);
        }
        results1.clear();
        
        processB.processElement(in, null, collector2);
        for (O2 result2 : results2) {
            ctx.output(output2, result2);
        }
        results1.clear();
    }
    

}
