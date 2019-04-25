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
package com.asakusafw.vanilla.client;

import static com.asakusafw.vanilla.client.VanillaConfiguration.*;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.io.File;
import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.Test;

import com.asakusafw.lang.utils.common.Optionals;

/**
 * Test for {@link VanillaConfiguration}.
 */
public class VanillaConfigurationTest {

    /**
     * default configurations.
     */
    @Test
    public void defaults() {
        VanillaConfiguration conf = new VanillaConfiguration();
        assertThat(conf.getNumberOfThreads(), is(DEFAULT_THREAD_COUNT));
        assertThat(conf.getNumberOfPartitions(), is(conf.getNumberOfThreads()));
        assertThat(conf.getBufferPoolSize(), is(DEFAULT_BUFFER_POOL_SIZE));
        assertThat(conf.getSwapDirectory(), is(DEFAULT_SWAP_DIRECTORY));
        assertThat(conf.getSwapDivision(), is(DEFAULT_SWAP_DIVISION));
        assertThat(conf.getOutputBufferSize(), is(DEFAULT_OUTPUT_BUFFER_SIZE));
        assertThat(conf.getOutputBufferFlush(), closeTo(DEFAULT_OUTPUT_BUFFER_FLUSH, 0.01));
        assertThat(conf.getOutputRecordSize(), is(DEFAULT_OUTPUT_RECORD_SIZE));
        assertThat(conf.getMergeThreshold(), is(DEFAULT_MERGE_THRESHOLD));
        assertThat(conf.getMergeFactor(), is(DEFAULT_MERGE_FACTOR));
    }

    /**
     * extract configurations.
     * @throws Exception if failed
     */
    @Test
    public void extract() throws Exception {
        File f = new File(".").getCanonicalFile();
        Map<String, Object> pairs = new LinkedHashMap<>();
        pairs.put(KEY_THREAD_COUNT, 2);
        pairs.put(KEY_PARTITION_COUNT, 3);
        pairs.put(KEY_BUFFER_POOL_SIZE, 4);
        pairs.put(KEY_OUTPUT_BUFFER_SIZE, 5);
        pairs.put(KEY_OUTPUT_BUFFER_FLUSH, 6);
        pairs.put(KEY_OUTPUT_RECORD_SIZE, 7);
        pairs.put(KEY_SWAP_DIVISION, 8);
        pairs.put(KEY_SWAP_DIRECTORY, f);
        pairs.put(KEY_MERGE_THRESHOLD, 9);
        pairs.put(KEY_MERGE_FACTOR, 10);

        VanillaConfiguration conf = VanillaConfiguration.extract(key -> Optionals.get(pairs, key)
                .map(String::valueOf));

        assertThat(conf.getNumberOfThreads(), is(2));
        assertThat(conf.getNumberOfPartitions(), is(3));
        assertThat(conf.getBufferPoolSize(), is(4L));
        assertThat(conf.getOutputBufferSize(), is(5));
        assertThat(conf.getOutputBufferFlush(), is(6d));
        assertThat(conf.getOutputRecordSize(), is(7));
        assertThat(conf.getSwapDivision(), is(8));
        assertThat(conf.getSwapDirectory().getCanonicalFile(), is(f));
        assertThat(conf.getMergeThreshold(), is(9));
        assertThat(conf.getMergeFactor(), is(10d));
    }

    /**
     * number of partitions from number of threads.
     */
    @Test
    public void mapping_partitions() {
        VanillaConfiguration conf = new VanillaConfiguration();
        conf.setNumberOfThreads(3);
        assertThat(conf.getNumberOfPartitions(), is(conf.getNumberOfThreads()));
    }

    /**
     * number of records from buffer size.
     */
    @Test
    public void infer_record_count() {
        VanillaConfiguration conf = new VanillaConfiguration();
        conf.setOutputBufferSize(1024_000);
        conf.setOutputRecordSize(1024);
        assertThat(conf.getNumberOfOutputRecords(), is(conf.getOutputBufferSize() / conf.getOutputRecordSize()));
    }
}
