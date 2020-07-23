package com.scaleunlimited.flinksnippets.sources;

import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.functions.source.FromElementsFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.junit.Assert;
import org.junit.Test;

public class SequentialSourcesTest {

    @Test
    public void testTwoSequencesInOrder() throws Exception {
        LocalStreamEnvironment env = new LocalStreamEnvironment();
        
        SourceFunction<String> source1 = sourceFromElements("1", "2", "3");
        SourceFunction<String> source2 = sourceFromElements("4", "5");
        SequentialSources<String> combo = new SequentialSources(TypeInformation.of(String.class), source1, source2);
        
        env.addSource(combo).print();
        
        try {
            env.execute();
        } catch (JobExecutionException e) {
            Assert.fail(e.getCause().getMessage());
        }
    }

    private SourceFunction<String> sourceFromElements(String... elements) throws IOException {
        return new FromElementsFunction<String>(BasicTypeInfo.STRING_TYPE_INFO.createSerializer(new ExecutionConfig()), elements);
    }

}
