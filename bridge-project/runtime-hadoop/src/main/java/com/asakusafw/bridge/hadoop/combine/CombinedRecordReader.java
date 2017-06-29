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
package com.asakusafw.bridge.hadoop.combine;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.asakusafw.runtime.stage.input.BridgeInputFormat.NullRecordReader;

/**
 * {@link RecordReader} for {@link CombinedInputSplit}.
 * @param <TKey> the key type
 * @param <TValue> the value type
 * @since 0.4.2
 */
public class CombinedRecordReader<TKey, TValue> extends RecordReader<TKey, TValue> {

    private final InputFormat<TKey, TValue> format;

    private Iterator<InputSplit> sources;

    private TaskAttemptContext context;

    private RecordReader<TKey, TValue> current;

    private boolean eof;

    private float progressPerSource;

    private float baseProgress;

    /**
     * Creates a new instance.
     * @param format the original input format
     */
    public CombinedRecordReader(InputFormat<TKey, TValue> format) {
        this.format = format;
    }

    @Override
    public void initialize(
            InputSplit split,
            TaskAttemptContext taskContext) throws IOException, InterruptedException {
        assert split instanceof CombinedInputSplit;
        List<InputSplit> combined = ((CombinedInputSplit) split).getSplits();
        this.sources = combined.iterator();
        this.context = taskContext;
        this.progressPerSource = combined.isEmpty() ? 1f : 1f / combined.size();
        this.baseProgress = 0f;
        prepare();
    }

    private void prepare() throws IOException, InterruptedException {
        if (current != null) {
            baseProgress += progressPerSource;
            current.close();
        }
        if (sources.hasNext()) {
            InputSplit next = sources.next();
            current = format.createRecordReader(next, context);
            current.initialize(next, context);
        } else {
            eof = true;
            current = new NullRecordReader<>();
        }
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        while (eof == false) {
            if (current.nextKeyValue()) {
                return true;
            }
            prepare();
        }
        return false;
    }

    @Override
    public TKey getCurrentKey() throws IOException, InterruptedException {
        return current.getCurrentKey();
    }

    @Override
    public TValue getCurrentValue() throws IOException, InterruptedException {
        return current.getCurrentValue();
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        float progress = current.getProgress();
        return baseProgress + progress * progressPerSource;
    }

    @Override
    public void close() throws IOException {
        if (current != null) {
            current.close();
        }
    }
}
