package com.scaleunlimited.flinksnippets.examples;

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.configuration.Configuration;

/**
 * Filter that will pass the first <limit> items. It assumes, if
 * parallelism > 1, that just prior to this filter the DataStream
 * has been randomly partitioned.
 *
 * @param <T>
 */
@SuppressWarnings("serial")
public class LimitFilter<T> extends RichFilterFunction<T> {
    
    private int _limit;
    
    private transient int _remainingRecords;
    
    public LimitFilter(int limit) {
        _limit = limit;
    }
    
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        
        // Calc number of records to return from this subtask
        int mySubtask = getRuntimeContext().getIndexOfThisSubtask();
        int remainingItems = _limit;
        int parallelism = getRuntimeContext().getNumberOfParallelSubtasks();
        for (int i = 0; i < parallelism; i++) {
            int remainingGroups = parallelism - i;
            int itemsInGroup = remainingItems / remainingGroups;
            if (i == mySubtask) {
                _remainingRecords = itemsInGroup;
                break;
            }
            
            remainingItems -= itemsInGroup;
        }
    }
    
    @Override
    public boolean filter(T value) throws Exception {
        if (_remainingRecords <= 0) {
            return false;
        }
        
        _remainingRecords--;
        return true;
    }

}
