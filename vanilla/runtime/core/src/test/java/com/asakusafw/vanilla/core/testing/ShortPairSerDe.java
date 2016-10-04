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
package com.asakusafw.vanilla.core.testing;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Comparator;

import com.asakusafw.dag.api.common.DataComparator;
import com.asakusafw.dag.api.common.KeyValueSerDe;
import com.asakusafw.lang.utils.common.Invariants;
import com.asakusafw.vanilla.core.io.KeyValueCursor;
import com.asakusafw.vanilla.core.util.Buffers;

/**
 * Ser/De {@code int} as short*short pair.
 */
public class ShortPairSerDe implements KeyValueSerDe {

    @Override
    public void serializeKey(Object object, DataOutput output) throws IOException, InterruptedException {
        int pair = (Integer) object;
        output.writeShort((short) (pair >> Short.SIZE));
    }

    @Override
    public void serializeValue(Object object, DataOutput output) throws IOException, InterruptedException {
        int pair = (Integer) object;
        output.writeShort((short) pair);
    }

    @Override
    public Object deserializeKey(DataInput keyInput) throws IOException, InterruptedException {
        return keyInput.readShort();
    }

    @Override
    public Object deserializePair(DataInput keyInput, DataInput valueInput) throws IOException, InterruptedException {
        return (keyInput.readShort() << Short.SIZE) | (valueInput.readShort() & 0xffff);
    }

    /**
     * Returns a new cursor.
     * @param values the values
     * @param offset the array offset
     * @param records the number of records from the offset
     * @return the created cursor
     */
    public static KeyValueCursor cursor(int[] values, int offset, int records) {
        return new KeyValueCursor() {
            private int recordIndex = offset;
            private ByteBuffer currentKey;
            private ByteBuffer currentValue;
            @Override
            public boolean next() throws IOException, InterruptedException {
                if (recordIndex >= offset + records) {
                    currentKey = null;
                    currentValue = null;
                    return false;
                }
                if (currentKey == null) {
                    currentKey = Buffers.allocate(Short.BYTES);
                    currentValue = Buffers.allocate(Short.BYTES);
                }
                long pair = values[recordIndex++];
                currentKey.clear();
                currentKey.putShort((short) (pair >> Short.SIZE));
                currentKey.flip();
                currentValue.clear();
                currentValue.putShort((short) pair);
                currentValue.flip();
                return true;
            }
            @Override
            public ByteBuffer getKey() throws IOException, InterruptedException {
                return Invariants.requireNonNull(currentKey);
            }
            @Override
            public ByteBuffer getValue() throws IOException, InterruptedException {
                return Invariants.requireNonNull(currentValue);
            }
            @Override
            public void close() throws IOException, InterruptedException {
                return;
            }
        };
    }

    /**
     * Returns a value comparator.
     * @return a value comparator
     */
    public static DataComparator dataComparator() {
        return (a, b) -> Short.compare(a.readShort(), b.readShort());
    }

    /**
     * Returns a record comparator.
     * @return a record comparator
     */
    public static Comparator<Integer> comparator() {
        ByteBuffer aBuf = Buffers.allocate(Short.BYTES);
        ByteBuffer bBuf = Buffers.allocate(Short.BYTES);
        return (a, b) -> {
            aBuf.clear();
            bBuf.clear();
            aBuf.putShort((short) (a >> Short.SIZE));
            bBuf.putShort((short) (b >> Short.SIZE));
            aBuf.flip();
            bBuf.flip();
            int keyDiff = aBuf.compareTo(bBuf);
            if (keyDiff != 0) {
                return keyDiff;
            }
            return Short.compare(a.shortValue(), b.shortValue());
        };
    }
}
