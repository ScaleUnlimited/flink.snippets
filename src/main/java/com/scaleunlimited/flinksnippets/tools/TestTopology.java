package com.scaleunlimited.flinksnippets.tools;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobSubmissionResult;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironmentWithAsyncExecution;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.scaleunlimited.flinksnippets.utils.FlinkUtils;
import com.scaleunlimited.flinksnippets.utils.UrlLogger;

/**
 * A Flink streaming workflow that can be executed.
 * 
 * State Checkpoints in Iterative Jobs
 * 
 * Flink currently only provides processing guarantees for jobs without iterations. Enabling checkpointing on an
 * iterative job causes an exception. In order to force checkpointing on an iterative program the user needs to set a
 * special flag when enabling checkpointing: env.enableCheckpointing(interval, force = true).
 * 
 * Please note that records in flight in the loop edges (and the state changes associated with them) will be lost during
 * failure.
 * 
 */
public class TestTopology {
	private static final Logger LOGGER = LoggerFactory.getLogger(TestTopology.class);

    private StreamExecutionEnvironment _env;
    private String _jobName;
    private JobID _jobID;
    
    protected TestTopology(StreamExecutionEnvironment env, String jobName) {
        _env = env;
        _jobName = jobName;
    }

    public void printDotFile(File outputFile) throws IOException {
    	String dotAsString = FlinkUtils.planToDot(_env.getExecutionPlan());
    	FileUtils.write(outputFile, dotAsString, "UTF-8");
    }
    
    public JobExecutionResult execute() throws Exception {
        return _env.execute(_jobName);
    }

    public JobSubmissionResult executeAsync() throws Exception {
    	if (!(_env instanceof LocalStreamEnvironmentWithAsyncExecution)) {
    		throw new IllegalStateException("StreamExecutionEnvironment must be LocalStreamEnvironmentWithAsyncExecution for async execution");
    	}
    	
    	LocalStreamEnvironmentWithAsyncExecution env = (LocalStreamEnvironmentWithAsyncExecution)_env;
    	JobSubmissionResult result = env.executeAsync(_jobName);
    	_jobID = result.getJobID();
    	return result;
    }
    
    public boolean isRunning() throws Exception {
    	if (!(_env instanceof LocalStreamEnvironmentWithAsyncExecution)) {
    		throw new IllegalStateException("StreamExecutionEnvironment must be LocalStreamEnvironmentWithAsyncExecution for async execution");
    	}
    	
    	LocalStreamEnvironmentWithAsyncExecution env = (LocalStreamEnvironmentWithAsyncExecution)_env;
    	return env.isRunning(_jobID);
    }
    
    public void stop() throws Exception {
    	if (!(_env instanceof LocalStreamEnvironmentWithAsyncExecution)) {
    		throw new IllegalStateException("StreamExecutionEnvironment must be LocalStreamEnvironmentWithAsyncExecution for async execution");
    	}
    	
    	LocalStreamEnvironmentWithAsyncExecution env = (LocalStreamEnvironmentWithAsyncExecution)_env;
    	env.stop(_jobID);
    	
    	// Wait for 5 seconds for the job to terminate.
    	long endTime = System.currentTimeMillis() + 5_000L;
    	while (env.isRunning(_jobID) && (System.currentTimeMillis() < endTime)) {
    		Thread.sleep(100L);
    	}
    	
    	// Stop the job execution environment.
    	env.stop();
    }
    
    public static class TestTopologyBuilder {

		public static final int DEFAULT_PARALLELISM = -1;

        private StreamExecutionEnvironment _env;
        private String _jobName = "flink.snippets";
        private int _parallelism = DEFAULT_PARALLELISM;
        
        private SinkFunction<String> _contentTextSink;
        private String _contentTextFilePathString;

        public TestTopologyBuilder(StreamExecutionEnvironment env) {
            _env = env;
        }

        public TestTopologyBuilder setJobName(String jobName) {
            _jobName = jobName;
            return this;
        }

        public TestTopologyBuilder setContentTextSink(SinkFunction<String> contentTextSink) {
        	if (_contentTextFilePathString != null) {
        		throw new IllegalArgumentException("already have a content text file path");
        	}
            _contentTextSink = contentTextSink;
            return this;
        }

        public TestTopologyBuilder setContentTextFile(String filePathString) {
        	if (_contentTextSink != null) {
        		throw new IllegalArgumentException("already have a content text sink");
        	}
            _contentTextFilePathString = filePathString;
            return this;
        }

        public TestTopologyBuilder setParallelism(int parallelism) {
            _parallelism = parallelism;
            return this;
        }

        @SuppressWarnings("serial")
        public TestTopology build() {
            if (_parallelism != DEFAULT_PARALLELISM) {
            	_env.setParallelism(_parallelism);
            }
            
            
            return new TestTopology(_env, _jobName);
        }
    }
    
	/**
	 * Trigger async execution, and then monitor the job. Fail if it the job is
	 * still running after <maxDurationMS> milliseconds.
	 * 
	 * @param maxDurationMS Maximum allowable execution time.
	 * @param maxQuietTimeMS Length of time w/no recorded activity after which we'll terminate.
	 * @throws Exception
	 */
	public void execute(int maxDurationMS, int maxQuietTimeMS) throws Exception {
		LOGGER.info("Starting async job {}", _jobName);
		
		executeAsync();
		
		boolean terminated = false;
		long endTime = System.currentTimeMillis() + maxDurationMS;
		while (System.currentTimeMillis() < endTime) {
			long lastActivityTime = UrlLogger.getLastActivityTime();
			if (lastActivityTime != UrlLogger.NO_ACTIVITY_TIME) {
				long curTime = System.currentTimeMillis();
				if ((curTime - lastActivityTime) > maxQuietTimeMS) {
					LOGGER.info("Stopping async job {} due to lack of activity", _jobName);

					stop();
					terminated = true;
					break;
				}
			}

			Thread.sleep(100L);
		}
		
		if (!terminated) {
			LOGGER.info("Stopping async job {} due to timeout", _jobName);
			stop();
			throw new RuntimeException("Job did not terminate in time");
		}
	}
}
