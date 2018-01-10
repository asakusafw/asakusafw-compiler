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

import com.asakusafw.lang.utils.common.InterruptibleIo;

/**
 * Accepts records.
 * @since 0.4.0
 */
public interface RecordSink extends InterruptibleIo {

    /**
     * Accepts key-value pair.
     * @param contents the contents buffer
     * @throws IOException if I/O error was occurred while processing the contents
     * @throws InterruptedException if interrupted while processing the contents
     */
    void accept(ByteBuffer contents) throws IOException, InterruptedException;

    /**
     * A stream of {@link RecordSink}.
     * @since 0.4.0
     */
    @FunctionalInterface
    interface Stream {

        /**
         * Offers the next sink.
         * @param recordCount the number of records
         * @param contentSize the total content size in bytes
         * @return the next sink
         * @throws IOException if I/O error was occurred while creating a sink
         * @throws InterruptedException if interrupted while creating a sink
         */
        RecordSink offer(int recordCount, int contentSize) throws IOException, InterruptedException;
    }
}
