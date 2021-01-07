/**
 * Copyright 2011-2021 Asakusa Framework Team.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.asakusafw.lang.compiler.mapreduce.testing;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.util.ReflectionUtils;

import com.asakusafw.runtime.io.ModelOutput;
import com.asakusafw.runtime.windows.WindowsConfigurator;

/**
 * Testing utilities for {@link OutputFormat}.
 * @since 0.5.0
 */
public class OutputFormatTester {

    static {
        WindowsConfigurator.install();
    }

    private final OutputFormat<?, ?> format;

    private final Configuration conf;

    /**
     *
     * Creates a new instance.
     * @param conf the current configuration
     * @param format the target format class
     */
    public OutputFormatTester(Configuration conf, Class<?> format) {
        this.conf = conf;
        this.format = (OutputFormat<?, ?>) ReflectionUtils.newInstance(format, conf);
    }

    /**
     * Write a series of records.
     * @param <T> the record type
     * @return the output sink
     * @throws IOException if failed
     * @throws InterruptedException if interrupted
     */
    public <T> ModelOutput<T> open() throws IOException, InterruptedException {
        TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID("t", 0, TaskType.MAP, 0, 0));
        format.checkOutputSpecs(context);

        OutputCommitter committer = format.getOutputCommitter(context);
        committer.setupJob(context);
        committer.setupTask(context);
        @SuppressWarnings("unchecked")
        RecordWriter<Object, T> writer = (RecordWriter<Object, T>) format.getRecordWriter(context);
        return new ModelOutput<T>() {
            @Override
            public void write(T model) throws IOException {
                try {
                    writer.write(NullWritable.get(), model);
                } catch (InterruptedException e) {
                    throw new AssertionError(e);
                }
            }
            @Override
            public void close() throws IOException {
                try {
                    writer.close(context);
                } catch (InterruptedException e) {
                    throw new AssertionError(e);
                }
                committer.commitTask(context);
                committer.commitJob(context);
            }
        };
    }
}
