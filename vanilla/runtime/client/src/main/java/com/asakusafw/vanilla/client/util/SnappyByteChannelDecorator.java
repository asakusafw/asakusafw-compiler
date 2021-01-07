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
package com.asakusafw.vanilla.client.util;

import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

import com.asakusafw.vanilla.core.io.ByteChannelDecorator;

/**
 * An implementation of {@link ByteChannelDecorator} which using snappy compression.
 * @since 0.5.3
 */
public class SnappyByteChannelDecorator implements ByteChannelDecorator {

    @Override
    public ReadableByteChannel decorate(ReadableByteChannel channel) {
        return new SnappyFramedReadableByteChannel(channel);
    }

    @Override
    public WritableByteChannel decorate(WritableByteChannel channel) {
        return new SnappyFramedWritableByteChannel(channel);
    }
}
