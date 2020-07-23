package com.scaleunlimited.flinksnippets.sources;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

@SuppressWarnings("serial")
public class SequentialSources<T> implements SourceFunction<T>, ResultTypeQueryable<T> {

    private TypeInformation<T> type;
    private SourceFunction<T>[] sources;
    private volatile boolean isRunning = true;

    @SafeVarargs
    public SequentialSources(TypeInformation<T> type, SourceFunction<T>...sources) {
        this.type = type;
        this.sources = sources;
    }
    
    @Override
    public void run(SourceContext<T> context) throws Exception {
        int index = 0;
        
        while (isRunning) {
            sources[index++].run(context);
            isRunning = index < sources.length;
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
        
        for (SourceFunction<T> source : sources) {
            source.cancel();
        }
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return type;
    }
}
