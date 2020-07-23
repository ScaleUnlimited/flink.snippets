package com.scaleunlimited.flink.lucenestate;

import java.io.IOException;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.OperatorStateBackend;

@SuppressWarnings("serial")
public class LuceneStateBackend extends AbstractStateBackend /* implements ConfigurableStateBackend */ {

    // ------------------------------------------------------------------------
    //  initialization and cleanup
    // ------------------------------------------------------------------------

    @Override
    public CheckpointStreamFactory createStreamFactory(JobID jobId, String operatorIdentifier) throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public CheckpointStreamFactory createSavepointStreamFactory(
            JobID jobId,
            String operatorIdentifier,
            String targetLocation) throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    // ------------------------------------------------------------------------
    //  state holding structures
    // ------------------------------------------------------------------------

    @Override
    public <K> AbstractKeyedStateBackend<K> createKeyedStateBackend(
            Environment env,
            JobID jobID,
            String operatorIdentifier,
            TypeSerializer<K> keySerializer,
            int numberOfKeyGroups,
            KeyGroupRange keyGroupRange,
            TaskKvStateRegistry kvStateRegistry) throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public OperatorStateBackend createOperatorStateBackend(
            Environment env,
            String operatorIdentifier)
            throws Exception {
        // TODO Auto-generated method stub
        return null;
    }


    
}
