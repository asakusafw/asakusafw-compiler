/**
 * Copyright 2011-2018 Asakusa Framework Team.
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
package com.asakusafw.vanilla.core.io;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import com.asakusafw.lang.utils.common.Arguments;
import com.asakusafw.lang.utils.common.Lang;

/**
 * An implementation of {@link KeyValueSink} which sort key-value pairs into individual partitions.
 * Each partition layout equals to {@link BasicKeyValueSink}.
 * @since 0.4.0
 */
public class KeyValuePartitioner implements KeyValueSink {

    private final int numberOfPartitions;

    private final KeyValueSink[] partitions;

    private int lastPartition = -1;

    /**
     * Creates a new instance.
     * @param channels the destination channels of individual partitions
     * @param size the maximum data size of each partition
     * @throws IOException if I/O error was occurred while initializing this object
     * @throws InterruptedException if interrupted while initializing this object
     */
    public KeyValuePartitioner(
            List<? extends DataWriter.Channel> channels,
            int size) throws IOException, InterruptedException {
        Arguments.require(channels.isEmpty() == false);
        this.numberOfPartitions = channels.size();
        this.partitions = new KeyValueSink[channels.size()];
        try (Closer closer = new Closer()) {
            int index = 0;
            for (DataWriter.Channel channel : channels) {
                // TODO more buffer efficient
                partitions[index++] = closer.add(new BasicKeyValueSink(channel, size));
            }
            closer.keep();
        }
    }

    /**
     * Creates a new stream of {@link KeyValuePartitioner}.
     * @param channels the destination channels of individual partitions
     * @return the created stream
     */
    public static Stream stream(List<? extends DataWriter.Channel> channels) {
        Arguments.requireNonNull(channels);
        Arguments.require(channels.isEmpty() == false);
        if (channels.size() == 1) {
            return BasicKeyValueSink.stream(channels.get(0));
        }
        return (recordCount, keySize, valueSize) -> {
            long total = 0;
            total += recordCount * Integer.BYTES * 3; // record_buffer.records[].{{key, values[]}.size, EOR}
            total += keySize;
            total += valueSize;
            total += Integer.BYTES; // EOF
            Arguments.require(total <= Integer.MAX_VALUE);
            return new KeyValuePartitioner(channels, (int) total);
        };
    }

    @Override
    public void accept(ByteBuffer key, ByteBuffer value) throws IOException, InterruptedException {
        int index = computeIndex(key);
        lastPartition = index;
        partitions[index].accept(key, value);
    }

    private int computeIndex(ByteBuffer key) {
        return (key.hashCode() & Integer.MAX_VALUE) % numberOfPartitions;
    }

    @Override
    public boolean accept(ByteBuffer value) throws IOException, InterruptedException {
        int index = lastPartition;
        if (index < 0) {
            return false;
        }
        return partitions[index].accept(value);
    }

    @Override
    public void close() throws IOException, InterruptedException {
        try (Closer closer = new Closer()) {
            Lang.forEach(partitions, closer::add);
        }
    }
}
