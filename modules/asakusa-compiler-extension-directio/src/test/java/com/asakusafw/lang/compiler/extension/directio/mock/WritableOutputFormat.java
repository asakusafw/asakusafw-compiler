package com.asakusafw.lang.compiler.extension.directio.mock;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.asakusafw.runtime.io.ModelOutput;

/**
 * An output format with {@link WritableModelOutput}.
 * @param <T> data model type
 */
public class WritableOutputFormat<T extends Writable> extends FileOutputFormat<NullWritable, T> {

    @Override
    public RecordWriter<NullWritable, T> getRecordWriter(TaskAttemptContext context) throws IOException,
            InterruptedException {
        Path path = getDefaultWorkFile(context, null);
        FileSystem fs = path.getFileSystem(context.getConfiguration());
        return new Writer<>(new WritableModelOutput<T>(fs.create(path, true)));
    }

    private static final class Writer<T extends Writable> extends RecordWriter<NullWritable, T> {

        private final ModelOutput<T> output;

        public Writer(ModelOutput<T> output) {
            this.output = output;
        }

        @Override
        public void write(NullWritable key, T value) throws IOException, InterruptedException {
            output.write(value);
        }

        @Override
        public void close(TaskAttemptContext context) throws IOException, InterruptedException {
            output.close();
        }
    }
}
