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
package com.asakusafw.vanilla.core.io;

import java.io.IOException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

/**
 * An implementation of {@link ByteChannelDecorator} which decorates nothing and just returns the bare channels.
 * @since 0.5.3
 */
public class NullByteChannelDecorator implements ByteChannelDecorator {

    /**
     * an instance of this class.
     */
    public static final NullByteChannelDecorator INSTANCE = new NullByteChannelDecorator();

    @Override
    public ReadableByteChannel decorate(ReadableByteChannel channel) throws IOException {
        return channel;
    }

    @Override
    public WritableByteChannel decorate(WritableByteChannel channel) throws IOException {
        return channel;
    }
}
