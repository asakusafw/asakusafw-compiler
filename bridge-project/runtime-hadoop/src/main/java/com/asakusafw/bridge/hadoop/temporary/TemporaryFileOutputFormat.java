/**
 * Copyright 2011-2019 Asakusa Framework Team.
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
package com.asakusafw.bridge.hadoop.temporary;

import java.io.IOException;
import java.text.MessageFormat;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.security.TokenCache;

import com.asakusafw.runtime.io.ModelOutput;
import com.asakusafw.runtime.stage.temporary.TemporaryStorage;

/**
 * A temporary output format.
 * @param <T> target type
 * @since 0.5.0
 */
public final class TemporaryFileOutputFormat<T> extends FileOutputFormat<NullWritable, T> {

    static final Log LOG = LogFactory.getLog(TemporaryFileOutputFormat.class);

    /**
     * The Hadoop property key of output name prefix.
     */
    public static final String KEY_FILE_NAME = BASE_OUTPUT_NAME;

    /**
     * The default output name prefix.
     */
    public static final String DEFAULT_FILE_NAME = PART;

    /**
     * Sets the output directory.
     * @param conf the current configuration
     * @param directory the target directory
     * @throws IOException if I/O error was occurred
     */
    public static void setOutputPath(Configuration conf, Path directory) throws IOException {
        Path q = directory.getFileSystem(conf).makeQualified(directory);
        conf.set(OUTDIR, q.toString());
      }

    @Override
    public void checkOutputSpecs(JobContext context) throws IOException {
        Path path = getOutputPath(context);
        if (path == null) {
            throw new IOException("Temporary output path is not set");
        }
        TokenCache.obtainTokensForNamenodes(
                context.getCredentials(),
                new Path[] { path },
                context.getConfiguration());
        if (path.getFileSystem(context.getConfiguration()).exists(path)) {
            throw new IOException(MessageFormat.format(
                    "Output directory {0} already exists",
                    path));
        }
    }

    @Override
    public RecordWriter<NullWritable, T> getRecordWriter(
            TaskAttemptContext context) throws IOException, InterruptedException {
        @SuppressWarnings("unchecked")
        Class<T> valueClass = (Class<T>) context.getOutputValueClass();
        String name = context.getConfiguration().get(KEY_FILE_NAME, DEFAULT_FILE_NAME);
        return createRecordWriter(context, name, valueClass);
    }

    /**
     * Creates a new {@link RecordWriter} to output temporary data.
     * @param <V> value type
     * @param context current context
     * @param name output name
     * @param dataType value type
     * @return the created writer
     * @throws IOException if failed to create a new {@link RecordWriter}
     * @throws InterruptedException if interrupted
     */
    public <V> RecordWriter<NullWritable, V> createRecordWriter(
            TaskAttemptContext context,
            String name,
            Class<V> dataType) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        FileOutputCommitter committer = (FileOutputCommitter) getOutputCommitter(context);
        Path file = new Path(
                committer.getWorkPath(),
                FileOutputFormat.getUniqueFile(context, name, "")); //$NON-NLS-1$
        ModelOutput<V> out = TemporaryStorage.openOutput(conf, dataType, file);
        return new RecordWriter<NullWritable, V>() {
            @Override
            public void write(NullWritable key, V value) throws IOException {
                out.write(value);
            }
            @Override
            public void close(TaskAttemptContext ignored) throws IOException {
                out.close();
            }
            @Override
            public String toString() {
                return String.format("TemporaryOutput(%s)", file); //$NON-NLS-1$
            }
        };
    }
}
