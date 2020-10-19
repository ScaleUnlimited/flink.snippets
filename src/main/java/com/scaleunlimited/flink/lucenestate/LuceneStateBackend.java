package com.scaleunlimited.flink.lucenestate;

import java.io.IOException;
import java.util.Collection;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.runtime.state.CheckpointStorage;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.CompletedCheckpointStorageLocation;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.OperatorStateBackend;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;

@SuppressWarnings("serial")
public class LuceneStateBackend extends AbstractStateBackend /* implements ConfigurableStateBackend */ {

    @Override
    public CheckpointStorage createCheckpointStorage(JobID arg0) throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public CompletedCheckpointStorageLocation resolveCheckpoint(String arg0) throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public <K> AbstractKeyedStateBackend<K> createKeyedStateBackend(Environment arg0, JobID arg1,
            String arg2, TypeSerializer<K> arg3, int arg4, KeyGroupRange arg5,
            TaskKvStateRegistry arg6, TtlTimeProvider arg7, MetricGroup arg8,
            Collection<KeyedStateHandle> arg9, CloseableRegistry arg10) throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public OperatorStateBackend createOperatorStateBackend(Environment arg0, String arg1,
            Collection<OperatorStateHandle> arg2, CloseableRegistry arg3) throws Exception {
        // TODO Auto-generated method stub
        return null;
    }

}
