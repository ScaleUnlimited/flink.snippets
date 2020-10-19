package com.scaleunlimited.flinksnippets;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A very simple, parallel source based on a list of elements. You can specify a delay
 * for between each element that is emitted.
 * 
 * @param <T>
 */
@SuppressWarnings("serial")
public class MockSource<T> extends RichParallelSourceFunction<T> implements ResultTypeQueryable<T>, Serializable {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(MockSource.class);
    
    private int listSize;
    private byte[] elementsSerialized;
    private TypeInformation<T> typeInfo;
    private TypeSerializer<T> serializer;
    private Time delay = null;
    private boolean neverTerminate = false;
    
    private transient int numSubtasks;
    private transient int mySubtaskIndex;
    
    private transient volatile boolean running;

    // Constructor for cases where you want an empty list as the source.
    public MockSource(TypeInformation<T> typeInfo) throws IOException {
        this(Collections.emptyList(), typeInfo);
    }

    @SuppressWarnings("unchecked")
    public MockSource(T... elements) throws IOException {
        this((List<T>) Arrays.asList(elements));
    }

    /**
     * Create a source from <data>, which cannot be empty (if so, use the other constructor that takes a typeInfo
     * argument.
     * 
     * @param data
     * @throws IOException
     */
    public MockSource(List<T> data) throws IOException {
        this(data, TypeExtractor.getForObject(data.get(0)));
    }

    public MockSource(List<T> data, TypeInformation<T> typeInfo) throws IOException {
        this.typeInfo = typeInfo;
        this.serializer = typeInfo.createSerializer(new ExecutionConfig());

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputViewStreamWrapper wrapper = new DataOutputViewStreamWrapper(baos);

        listSize = 0;
        try {
            for (T element : data) {
                serializer.serialize(element, wrapper);
                listSize++;
            }
        } catch (Exception e) {
            throw new IOException("Serializing the source elements failed: " + e.getMessage(), e);
        }

        this.elementsSerialized = baos.toByteArray();
    }

    public MockSource<T> setDelay(Time delay) {
        this.delay = delay;
        return this;
    }
    
    public MockSource<T> setNeverTerminate(boolean neverTerminate) {
        this.neverTerminate = neverTerminate;
        return this;
    }
    
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        
        numSubtasks = getRuntimeContext().getNumberOfParallelSubtasks();
        mySubtaskIndex = getRuntimeContext().getIndexOfThisSubtask();
    }
    
    @Override
    public void run(SourceContext<T> ctx) throws Exception {
        running = true;

        ByteArrayInputStream bais = new ByteArrayInputStream(elementsSerialized);
        final DataInputView input = new DataInputViewStreamWrapper(bais);

        if (delay != null) {
            LOGGER.debug("MockSource initial delay for {}ms", delay.toMilliseconds());
            Thread.sleep(delay.toMilliseconds());
        }

        int i = 0;
        while (running && ((i < this.listSize) || neverTerminate)) {
            if (i < this.listSize) {
                T next;
                try {
                    next = serializer.deserialize(input);
                } catch (Exception e) {
                    throw new IOException("Failed to deserialize an element from the source. "
                            + "If you are using user-defined serialization (Value and Writable types), check the "
                            + "serialization functions.\nSerializer is " + serializer, e);
                }

                if ((i % numSubtasks) == mySubtaskIndex) {
                    ctx.collect(next);
                }

                i++;
            }
            
            if (delay != null) {
                LOGGER.debug("MockSource delaying for {}ms", delay.toMilliseconds());

                Thread.sleep(delay.toMilliseconds());
            }
        }
    }

    @Override
    public void cancel() {
        running = false;
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return typeInfo;
    }
}