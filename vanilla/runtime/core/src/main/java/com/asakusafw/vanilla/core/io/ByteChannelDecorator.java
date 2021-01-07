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
package com.asakusafw.vanilla.core.io;

import java.io.IOException;
import java.nio.channels.ByteChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

import com.asakusafw.dag.api.processor.ProcessorContext;

/**
 * Decorates {@link ByteChannel}s.
 * @since 0.5.3
 */
public interface ByteChannelDecorator {

    /**
     * initializes this object.
     * @param context the current context
     * @throws IOException if I/O error occurred while initializing the decorator
     * @throws InterruptedException if interrupted while initializing the decorator
     */
    default void initialize(ProcessorContext context) throws IOException, InterruptedException {
        return;
    }

    /**
     * Decorates the given channel.
     * @param channel the source channel
     * @return the decorated channel
     * @throws IOException if I/O error was occurred while decorating the channel
     * @throws InterruptedException if interrupted while decorating the channel
     */
    ReadableByteChannel decorate(ReadableByteChannel channel) throws IOException, InterruptedException;

    /**
     * Decorates the given channel.
     * @param channel the source channel
     * @return the decorated channel
     * @throws IOException if I/O error was occurred while decorating the channel
     * @throws InterruptedException if interrupted while decorating the channel
     */
    WritableByteChannel decorate(WritableByteChannel channel) throws IOException, InterruptedException;
}
