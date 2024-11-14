package com.example;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class IncrementalNumberPrintingJob {
    public static void main(String[] args) throws Exception {
        // Set up the streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Create a custom source that emits incremental numbers every second
        env.addSource(new SourceFunction<Long>() {
                    private volatile boolean isRunning = true;
                    private long number = 1;

                    @Override
                    public void run(SourceContext<Long> ctx) throws Exception {
                        while (isRunning) {
                            ctx.collect(number);
                            number++;
                            Thread.sleep(1000);  // Wait for 1 second
                        }
                    }

                    @Override
                    public void cancel() {
                        isRunning = false;
                    }
                }).name("Incremental Number Generator")
                .setParallelism(1)
                .print()
                .name("Number Printer");

        // Execute the job
        env.execute("Incremental Number Printing Job");
    }
}