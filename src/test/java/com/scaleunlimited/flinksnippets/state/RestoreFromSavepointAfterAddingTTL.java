package com.scaleunlimited.flinksnippets.state;

import org.apache.flink.api.common.JobExecutionResult;
import org.assertj.core.util.Files;
import org.junit.jupiter.api.Test;

import java.io.File;

public class RestoreFromSavepointAfterAddingTTL {

    @Test
    public void restoreAfterAddingTTL() throws Exception {
        File testDir = Files.newTemporaryFolder();
        File savepointDir = new File(testDir, "savepoint");
        savepointDir.mkdir();

        // First run without TTL, and take a savepoint

        // Now run with TTL, from a savepoint

        // Verify no errors.
        
        env.setDefaultSavepointDirectory(savepointDir);

        JobExecutionResult result = null;
        if (startfromSavepointDir != null) {
            StreamGraph streamGraph = env.getStreamGraph();
            streamGraph.setSavepointRestoreSettings(
                    SavepointRestoreSettings.forPath(startfromSavepointDir));
            streamGraph.setJobName(JOB_NAME);
            result = env.execute(streamGraph);
        } else {
            result = execute();
        }

    }

    private static class SimpleStatefulJob {

        public JobExecutionResult run(boolean withTTL, boolean withSavepoint, File savepointDir) {

        }
        public static void main(String[] args) {

        }
    }
}
