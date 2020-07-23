package com.scaleunlimited.flinksnippets.tools;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import com.scaleunlimited.flinksnippets.tools.TestTopology.TestTopologyBuilder;

public class TestTool {

	public static final long DO_NOT_FORCE_CRAWL_DELAY = -1L;

	public static class TestToolOptions {
		
        private int _parallelism = TestTopologyBuilder.DEFAULT_PARALLELISM;
        private String _outputFile = null;
        
		@Option(name = "-parallelism", usage = "Flink paralellism", required = false)
	    public void setParallelism(int parallelism) {
			_parallelism = parallelism;
	    }
		
		@Option(name = "-outputfile", usage = "Local file to store test output", required = false)
	    public void setOutputFile(String outputFile) {
			_outputFile = outputFile;
	    }
		
		public int getParallelism() {
			return _parallelism;
		}
		
		public String getOutputFile() {
			return _outputFile;
		}
	}
	
    private static void printUsageAndExit(CmdLineParser parser) {
        parser.printUsage(System.err);
        System.exit(-1);
    }
	
	public static void main(String[] args) {
		
        // Dump the classpath to stdout to debug artifact version conflicts
		System.out.println(    "Java classpath: "
                    		+   System.getProperty("java.class.path", "."));

        TestToolOptions options = new TestToolOptions();
        CmdLineParser parser = new CmdLineParser(options);

        try {
            parser.parseArgument(args);
        } catch (CmdLineException e) {
            System.err.println(e.getMessage());
            printUsageAndExit(parser);
        }

		// Generate topology, run it
        
		try {
			StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
	        run(env, options);
		} catch (Throwable t) {
			System.err.println("Error running TestTool: " + t.getMessage());
			t.printStackTrace(System.err);
			System.exit(-1);
		}
	}

	public static void run(StreamExecutionEnvironment env, TestToolOptions options) throws Exception {
		TestTopologyBuilder builder = new TestTopologyBuilder(env)
			.setParallelism(options.getParallelism());
		
		if (options.getOutputFile() != null) {
			builder.setContentTextFile(options.getOutputFile());
		}
		
		builder.build().execute();
	}
	
}
