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
package com.asakusafw.vanilla.core.io;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import com.asakusafw.dag.api.common.DataComparator;
import com.asakusafw.lang.utils.buffer.nio.NioDataBuffer;
import com.asakusafw.lang.utils.common.Arguments;
import com.asakusafw.lang.utils.common.InterruptibleIo;
import com.asakusafw.lang.utils.common.Lang;

/**
 * Merges set of sorted {@link KeyValueCursor}.
 * @since 0.4.0
 */
public class KeyValueMerger implements KeyValueCursor {

    private final HeapElement[] heap;

    private final DataComparator comparator;

    private boolean firstTime = true;

    /**
     * Creates a new instance.
     * @param sortedCursors the sorted key-value cursors, must not be empty, and recommended {@code >= 2} cursors
     * @param comparator the value comparator (nullable)
     */
    public KeyValueMerger(List<? extends KeyValueCursor> sortedCursors, DataComparator comparator) {
        Arguments.requireNonNull(sortedCursors);
        Arguments.require(sortedCursors.isEmpty() == false);
        this.heap = sortedCursors.stream()
                .map(HeapElement::new)
                .toArray(HeapElement[]::new);
        this.comparator = comparator;
    }

    @Override
    public boolean next() throws IOException, InterruptedException {
        HeapElement[] h = heap;
        if (firstTime) {
            firstTime = false;
            for (int i = 0; i < h.length; i++) {
                h[i].fill();
            }
            for (int i = h.length / 2; i >= 0; i--) {
                shiftDown(i);
            }
        } else {
            h[0].fill();
            shiftDown(0);
        }
        return h[0].key != null;
    }

    private void shiftDown(int i) throws IOException {
        HeapElement[] h = heap;
        int length = h.length;
        int current = i;
        while (true) {
            int left = (current << 1) + 1;
            int right = left + 1;
            if (left < length && isViolate(h[current], h[left])) {
                if (right < length && isViolate(h[left], h[right])) {
                    swap(current, right);
                    current = right;
                } else {
                    swap(current, left);
                    current = left;
                }
            } else {
                if (right < length && isViolate(h[current], h[right])) {
                    swap(current, right);
                    current = right;
                } else {
                    break;
                }
            }
        }
    }

    private boolean isViolate(HeapElement parent, HeapElement node) throws IOException {
        return parent.isViolate(comparator, node);
    }

    private void swap(int i, int j) {
        HeapElement[] h = heap;
        HeapElement t = h[i];
        h[i] = h[j];
        h[j] = t;
    }

    @Override
    public ByteBuffer getKey() throws IOException, InterruptedException {
        return heap[0].key;
    }

    @Override
    public ByteBuffer getValue() throws IOException, InterruptedException {
        return heap[0].value;
    }

    @Override
    public void close() throws IOException, InterruptedException {
        try (Closer closer = new Closer()) {
            Lang.forEach(heap, closer::add);
        }
    }

    @Override
    public String toString() {
        return Arrays.toString(heap);
    }

    private static final class HeapElement implements InterruptibleIo {

        private final KeyValueCursor entity;

        ByteBuffer key;

        ByteBuffer value;

        private final NioDataBuffer wrapper = new NioDataBuffer();

        private boolean closed = false;

        HeapElement(KeyValueCursor entity) {
            this.entity = entity;
        }

        void fill() throws IOException, InterruptedException {
            if (closed) {
                return;
            }
            if (entity.next()) {
                key = entity.getKey();
                value = entity.getValue();
                wrapper.contents = value;
                value.mark();
            } else {
                key = null;
                value = null;
                wrapper.contents = NioDataBuffer.EMPTY_BUFFER;
                close();
            }
        }

        boolean isViolate(DataComparator comparator, HeapElement node) throws IOException {
            ByteBuffer k1 = key;
            ByteBuffer k2 = node.key;
            if (k1 == null) {
                if (k2 == null) {
                    return false;
                }
                return true;
            } else if (k2 == null) {
                return false;
            }
            int kDiff = k1.compareTo(k2);
            if (kDiff != 0) {
                return kDiff > 0;
            }
            if (comparator == null) {
                return false;
            }
            NioDataBuffer v1 = wrapper;
            NioDataBuffer v2 = node.wrapper;
            boolean violate = comparator.compare(v1, v2) > 0;
            v1.contents.reset();
            v2.contents.reset();
            return violate;
        }

        @Override
        public void close() throws IOException, InterruptedException {
            if (closed == false) {
                entity.close();
                closed = true;
            }
        }
    }
}
