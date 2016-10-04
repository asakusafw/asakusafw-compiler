/**
 * Copyright 2011-2016 Asakusa Framework Team.
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
package com.asakusafw.dag.runtime.table;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.io.Writable;

import com.asakusafw.dag.runtime.adapter.KeyBuffer;
import com.asakusafw.lang.utils.buffer.nio.ResizableNioDataBuffer;

/**
 * An implementation of {@link com.asakusafw.dag.runtime.adapter.KeyBuffer KeyBuffer} using Java NIO.
 * @since 0.4.0
 */
public class NioKeyBuffer implements KeyBuffer {

    private final ResizableNioDataBuffer buffer = new ResizableNioDataBuffer();

    private final DirectView directView = new DirectView(buffer);

    @Override
    public View getView() {
        return directView;
    }

    @Override
    public View getFrozen() {
        ByteBuffer contents = buffer.contents;
        ByteBuffer copy = ByteBuffer.allocateDirect(contents.position()).order(contents.order());

        contents.flip();
        copy.put(contents);
        contents.position(contents.limit()).limit(contents.capacity());
        return new FrozenView(copy);
    }

    @Override
    public KeyBuffer clear() {
        buffer.contents.clear();
        return this;
    }

    @Override
    public KeyBuffer append(Object value) {
        try {
            ((Writable) value).write(buffer);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        return this;
    }

    private abstract static class ViewBase implements View {

        ViewBase() {
            return;
        }

        abstract ByteBuffer contents();

        @Override
        public final int hashCode() {
            ByteBuffer b = contents();
            int limit = b.position();

            int result = 0;
            int offset = 0;
            for (int n = limit - Long.BYTES; offset <= n; offset += Long.BYTES) {
                result = result * 31 + Long.hashCode(b.getLong(offset));
            }
            for (int n = limit - Integer.BYTES; offset <= n; offset += Integer.BYTES) {
                result = result * 31 + Integer.hashCode(b.getInt(offset));
            }
            for (int n = limit; offset < n; offset++) {
                result = result * 31 + Byte.hashCode(b.get(offset));
            }
            return result;
        }

        @Override
        public final boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if ((obj instanceof ViewBase) == false) {
                return false;
            }
            ByteBuffer a = contents();
            ByteBuffer b = ((ViewBase) obj).contents();
            int limit = a.position();
            if (limit != b.position()) {
                return false;
            }
            int offset = 0;
            for (int n = limit - Long.BYTES; offset <= n; offset += Long.BYTES) {
                if (a.getLong(offset) != b.getLong(offset)) {
                    return false;
                }
            }
            for (int n = limit - Integer.BYTES; offset <= n; offset += Integer.BYTES) {
                if (a.getInt(offset) != b.getInt(offset)) {
                    return false;
                }
            }
            for (int n = limit; offset < n; offset++) {
                if (a.get(offset) != b.get(offset)) {
                    return false;
                }
            }
            return true;
        }
    }

    private static final class DirectView extends ViewBase {

        private final ResizableNioDataBuffer buffer;

        DirectView(ResizableNioDataBuffer buffer) {
            this.buffer = buffer;
        }

        @Override
        ByteBuffer contents() {
            return buffer.contents;
        }
    }

    private static final class FrozenView extends ViewBase {

        private final ByteBuffer buffer;

        FrozenView(ByteBuffer buffer) {
            this.buffer = buffer;
            assert buffer.limit() == buffer.capacity();
        }

        @Override
        ByteBuffer contents() {
            return buffer;
        }
    }
}
