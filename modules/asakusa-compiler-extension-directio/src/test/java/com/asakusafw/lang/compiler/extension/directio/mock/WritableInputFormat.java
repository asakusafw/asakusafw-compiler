package com.asakusafw.lang.compiler.extension.directio.mock;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * An input format with {@link WritableModelInput}.
 * @param <T> data model type
 */
public class WritableInputFormat<T extends Writable> extends FileInputFormat<NullWritable, T> {

    @Override
    public RecordReader<NullWritable, T> createRecordReader(
            InputSplit split,
            TaskAttemptContext context) throws IOException, InterruptedException {
        return new Reader<>();
    }

    private static final class Reader<T extends Writable> extends RecordReader<NullWritable, T> {

        private Configuration conf;

        private WritableModelInput<T> input;

        private T buffer;

        public Reader() {
            return;
        }

        @Override
        public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
            conf = context.getConfiguration();
            FileSplit s = (FileSplit) split;
            Path path = s.getPath();
            FileSystem fs = path.getFileSystem(conf);
            input = new WritableModelInput<>(fs.open(path));
        }

        @Override
        public NullWritable getCurrentKey() throws IOException, InterruptedException {
            return NullWritable.get();
        }

        @Override
        public T getCurrentValue() throws IOException, InterruptedException {
            return buffer;
        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
            return 0;
        }

        @SuppressWarnings("unchecked")
        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            if (buffer == null) {
                String className = input.getClassName();
                if (className == null) {
                    return false;
                }
                try {
                    Class<?> aClass = conf.getClassByName(className);
                    buffer = (T) aClass.getConstructor().newInstance();
                } catch (ReflectiveOperationException e) {
                    throw new IOException(e);
                }
            }
            return input.readTo(buffer);
        }

        @Override
        public void close() throws IOException {
            if (input != null) {
                input.close();
            }
        }
    }
}
