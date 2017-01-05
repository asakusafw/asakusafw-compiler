/**
 * Copyright 2011-2017 Asakusa Framework Team.
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
package com.asakusafw.bridge.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.asakusafw.runtime.directio.Counter;
import com.asakusafw.runtime.io.ModelInput;

/**
 * An implementation of {@link RecordReader} for reading objects from {@link ModelInput}.
 * @param <T> the target data model type
 */
public final class ModelInputRecordReader<T> extends RecordReader<NullWritable, Object> {

    private static final NullWritable KEY = NullWritable.get();

    private final ModelInput<T> input;

    private final T buffer;

    private final Counter sizeCounter;

    private final double fragmentSize;

    private boolean closed = false;

    /**
     * Creates a new instance.
     * @param input the source {@link ModelInput}
     * @param buffer the buffer data model object
     * @param sizeCounter the size counter
     * @param fragmentSize the fragment size in bytes
     */
    public ModelInputRecordReader(ModelInput<T> input, T buffer, Counter sizeCounter, long fragmentSize) {
        assert input != null;
        assert buffer != null;
        assert sizeCounter != null;
        this.sizeCounter = sizeCounter;
        this.input = input;
        this.buffer = buffer;
        if (fragmentSize <= 0) {
            this.fragmentSize = Double.POSITIVE_INFINITY;
        } else {
            this.fragmentSize = fragmentSize;
        }
    }

    @Override
    public void initialize(
            InputSplit split,
            TaskAttemptContext context) throws IOException, InterruptedException {
        return;
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (closed) {
            return false;
        }
        boolean exists = input.readTo(buffer);
        if (exists == false) {
            return false;
        }
        return exists;
    }

    @Override
    public NullWritable getCurrentKey() throws IOException, InterruptedException {
        return KEY;
    }

    @Override
    public Object getCurrentValue() throws IOException, InterruptedException {
        return buffer;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        if (closed) {
            return 1.0f;
        }
        float progress = (float) (sizeCounter.get() / (fragmentSize + 1));
        return Math.min(progress, 0.99f);
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        input.close();
    }
}