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

import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.junit.Test;

import com.asakusafw.lang.compiler.mapreduce.testing.InputFormatTester;
import com.asakusafw.runtime.directio.Counter;
import com.asakusafw.runtime.io.ModelInput;

/**
 * Test for {@link ModelInputRecordReader}.
 */
public class ModelInputRecordReaderTest {

    private final StringCollector collector = new StringCollector();

    /**
     * simple case.
     * @throws Exception if failed
     */
    @Test
    public void simple() throws Exception {
        Configuration conf = new Configuration();
        InputFormatTester tester = new InputFormatTester(conf, new Dummy("Hello, world!"));
        tester.collect(collector);

        assertThat(collector.results, containsInAnyOrder("Hello, world!"));
    }

    /**
     * empty values.
     * @throws Exception if failed
     */
    @Test
    public void empty_values() throws Exception {
        Configuration conf = new Configuration();
        InputFormatTester tester = new InputFormatTester(conf, new Dummy());
        tester.collect(collector);

        assertThat(collector.results, hasSize(0));
    }

    /**
     * multiple values.
     * @throws Exception if failed
     */
    @Test
    public void multiple() throws Exception {
        Configuration conf = new Configuration();
        InputFormatTester tester = new InputFormatTester(conf, new Dummy("Hello1", "Hello2", "Hello3"));
        tester.collect(collector);

        assertThat(collector.results, containsInAnyOrder("Hello1", "Hello2", "Hello3"));
    }

    static class StringCollector implements Consumer<StringBuilder> {

        final Set<String> results = new HashSet<>();

        @Override
        public void accept(StringBuilder object) {
            results.add(object.toString());
        }
    }

    private static class StringInput implements ModelInput<StringBuilder> {

        private final Iterator<String> values;

        public StringInput(Iterable<String> values) {
            this.values = values.iterator();
        }

        @Override
        public boolean readTo(StringBuilder model) throws IOException {
            if (values.hasNext()) {
                String value = values.next();
                model.setLength(0);
                model.append(value);
                return true;
            }
            return false;
        }

        @Override
        public void close() throws IOException {
            return;
        }
    }

    private static class Dummy extends InputFormat<NullWritable, Object> {

        private final Iterable<String> values;

        public Dummy(String... values) {
            this.values = Arrays.asList(values);
        }

        @Override
        public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
            return Collections.singletonList(new InputSplit() {
                @Override
                public long getLength() throws IOException, InterruptedException {
                    return 0;
                }
                @Override
                public String[] getLocations() throws IOException, InterruptedException {
                    return null;
                }
            });
        }

        @Override
        public RecordReader<NullWritable, Object> createRecordReader(
                InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
            return new ModelInputRecordReader<>(new StringInput(values), new StringBuilder(), new Counter(), 0);
        }
    }
}
