package com.scaleunlimited.flinksnippets.examples;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.junit.Test;

public class FileSinkTest {

    @Test
    public void test() throws Exception {
        File testDir = new File("build/test/FlinkSinkTest/test/");
        FileUtils.deleteDirectory(testDir);
        testDir.mkdirs();

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);
        
        // Set completed file name to end with ".txt"
        OutputFileConfig outputConfig = OutputFileConfig.builder()
                .withPartSuffix(".txt")
                .build();

        FileSink<String> sink =
                FileSink.forRowFormat(
                                new Path(testDir.toURI()),
                                new SimpleStringEncoder<String>())
                        .withOutputFileConfig(outputConfig)
                        .build();

        env.fromElements("message 1", "message 2", "message 3", "message 4")
            .sinkTo(sink);
        
        env.execute();
        
        // We will wind up with a time-stamped directory inside of <testDir>, e.g.
        // "2023-05-16--16", which  contains one text file (because our parallelism
        // is 1), with a name like:
        //
        // part-0249f740-6f13-419d-b5d0-9230101f107b-0.txt
    }
    
}
