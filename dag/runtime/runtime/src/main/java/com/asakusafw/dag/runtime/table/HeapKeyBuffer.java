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
package com.asakusafw.dag.runtime.table;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.Writable;

import com.asakusafw.dag.runtime.adapter.KeyBuffer;
import com.asakusafw.runtime.io.util.DataBuffer;

/**
 * On-heap implementation of {@link com.asakusafw.dag.runtime.adapter.KeyBuffer KeyBuffer}.
 * @since 0.4.0
 */
public class HeapKeyBuffer implements KeyBuffer, KeyBuffer.View {

    final DataBuffer buffer = new DataBuffer();

    @Override
    public KeyBuffer clear() {
        buffer.reset(0, 0);
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

    @Override
    public KeyBuffer.View getView() {
        return this;
    }

    @Override
    public KeyBuffer.View getFrozen() {
        return FrozenView.build(buffer);
    }

    @Override
    public int hashCode() {
        DataBuffer b = buffer;
        return hashCodeInBytes(b.getData(), b.getReadPosition(), b.getReadLimit());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj instanceof HeapKeyBuffer) {
            DataBuffer b1 = buffer;
            DataBuffer b2 = ((HeapKeyBuffer) obj).buffer;
            return equalsInBytes(
                    b1.getData(), b1.getReadPosition(), b1.getReadLimit(),
                    b2.getData(), b2.getReadPosition(), b2.getReadLimit());
        } else if (obj instanceof FrozenView) {
            DataBuffer b1 = buffer;
            byte[] b2 = ((FrozenView) obj).buffer;
            return equalsInBytes(
                    b1.getData(), b1.getReadPosition(), b1.getReadLimit(),
                    b2, 0, b2.length);
        } else {
            return false;
        }
    }

    @Override
    public String toString() {
        return String.format(
                "HeapKeyBuffer(size=%d, hash=%x)", //$NON-NLS-1$
                buffer.getReadRemaining(),
                hashCode());
    }

    private static final class FrozenView implements KeyBuffer.View {

        private static final FrozenView EMPTY = new FrozenView(new byte[0]);

        final byte[] buffer;

        private FrozenView(byte[] buffer) {
            this.buffer = buffer;
        }

        static FrozenView build(DataBuffer entity) {
            int from = entity.getReadPosition();
            int to = entity.getReadLimit();
            if (from == to) {
                return EMPTY;
            } else {
                return new FrozenView(Arrays.copyOfRange(entity.getData(), from, to));
            }
        }

        @Override
        public int hashCode() {
            byte[] b = buffer;
            return hashCodeInBytes(b, 0, b.length);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj instanceof HeapKeyBuffer) {
                byte[] b1 = buffer;
                DataBuffer b2 = ((HeapKeyBuffer) obj).buffer;
                return equalsInBytes(
                        b1, 0, b1.length,
                        b2.getData(), b2.getReadPosition(), b2.getReadLimit());
            } else if (obj instanceof FrozenView) {
                byte[] b1 = buffer;
                byte[] b2 = ((FrozenView) obj).buffer;
                return equalsInBytes(
                        b1, 0, b1.length,
                        b2, 0, b2.length);
            } else {
                return false;
            }
        }

        @Override
        public String toString() {
            return String.format(
                    "HeapKey(size=%d, hash=%x)", //$NON-NLS-1$
                    buffer.length,
                    hashCode());
        }
    }

    static int hashCodeInBytes(byte[] buffer, int from, int to) {
        final int prime = 31;
        int result = 1;
        for (int i = from; i < to; i++) {
            result = result * prime + buffer[i];
        }
        return result;
    }

    static boolean equalsInBytes(byte[] b1, int from1, int to1, byte[] b2, int from2, int to2) {
        if (to1 - from1 != to2 - from2) {
            return false;
        }
        for (int i = 0, n = to1 - from1; i < n; i++) {
            if (b1[from1 + i] != b2[from2 + i]) {
                return false;
            }
        }
        return true;
    }
}
