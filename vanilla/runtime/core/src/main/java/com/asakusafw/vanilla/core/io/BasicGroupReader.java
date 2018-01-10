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
import java.util.Objects;

import com.asakusafw.dag.api.common.KeyValueDeserializer;
import com.asakusafw.dag.api.processor.GroupReader;
import com.asakusafw.dag.api.processor.ObjectReader;
import com.asakusafw.lang.utils.buffer.nio.NioDataBuffer;
import com.asakusafw.lang.utils.common.Arguments;
import com.asakusafw.lang.utils.common.Invariants;
import com.asakusafw.vanilla.core.util.Buffers;

/**
 * A basic implementation of {@link ObjectReader} using {@link KeyValueCursor}.
 * @since 0.4.0
 */
public class BasicGroupReader implements GroupReader {

    private final KeyValueCursor input;

    private final KeyValueDeserializer deserializer;

    private final Group group;

    private final NioDataBuffer keyWrapper = new NioDataBuffer();

    private final NioDataBuffer valueWrapper = new NioDataBuffer();

    private Status prepared;

    private Object currentObject;

    /**
     * Creates a new instance.
     * @param input the input cursor
     * @param deserializer the object deserializer
     */
    public BasicGroupReader(KeyValueCursor input, KeyValueDeserializer deserializer) {
        Arguments.requireNonNull(input);
        Arguments.requireNonNull(deserializer);
        this.input = input;
        this.deserializer = deserializer;
        this.group = new Group(deserializer);
    }

    private Status prepare() throws IOException, InterruptedException {
        Status next = prepared;
        if (prepared != null) {
            prepared = null;
            return next;
        }
        if (input.next()) {
            ByteBuffer lastKey = group.key;
            ByteBuffer nextKey = input.getKey();
            if (lastKey == null || lastKey.equals(nextKey) == false) {
                group.put(nextKey);
                return Status.GROUP_BEGIN;
            } else {
                return Status.GROUP_MEMBERS;
            }
        } else {
            group.key = null;
            return Status.EOF;
        }
    }

    private void prepared(Status status) {
        prepared = status;
    }

    @Override
    public boolean nextGroup() throws IOException, InterruptedException {
        while (true) {
            Status status = prepare();
            switch (status) {
            case EOF:
                currentObject = null;
                prepared(Status.EOF);
                return false;
            case GROUP_BEGIN:
                currentObject = null;
                prepared(Status.GROUP_MEMBERS);
                return true;
            case GROUP_MEMBERS:
                // skip until beginning of the next group
                continue;
            default:
                throw new AssertionError(status);
            }
        }
    }

    @Override
    public boolean nextObject() throws IOException, InterruptedException {
        Status status = prepare();
        switch (status) {
        case EOF:
        case GROUP_BEGIN:
            currentObject = null;
            prepared(status);
            return false;
        case GROUP_MEMBERS:
            keyWrapper.contents = input.getKey();
            valueWrapper.contents = input.getValue();
            currentObject = deserializer.deserializePair(keyWrapper, valueWrapper);
            keyWrapper.contents = NioDataBuffer.EMPTY_BUFFER;
            valueWrapper.contents = NioDataBuffer.EMPTY_BUFFER;
            return true;
        default:
            throw new AssertionError(status);
        }
    }

    @Override
    public GroupInfo getGroup() throws IOException, InterruptedException {
        Invariants.requireNonNull(group.key);
        return group;
    }

    @Override
    public Object getObject() throws IOException, InterruptedException {
        Invariants.requireNonNull(currentObject);
        return currentObject;
    }

    @Override
    public void close() throws IOException, InterruptedException {
        input.close();
    }

    private enum Status {

        EOF,

        GROUP_BEGIN,

        GROUP_MEMBERS,
    }

    private static final class Group implements GroupInfo {

        private static final int KEY_BUFFER_MIN_SIZE = 256;

        private static final int KEY_BUFFER_MARGIN = 16;

        private final KeyValueDeserializer deserializer;

        private final NioDataBuffer wrapper = new NioDataBuffer();

        ByteBuffer key;

        Group(KeyValueDeserializer deserializer) {
            this.deserializer = deserializer;
        }

        void put(ByteBuffer newKey) {
            if (key == null || key.capacity() < newKey.remaining()) {
                key = Buffers.allocate(Math.max(KEY_BUFFER_MIN_SIZE, newKey.remaining() + KEY_BUFFER_MARGIN));
            }
            key.clear();
            newKey.mark();
            key.put(newKey);
            newKey.reset();
            key.flip();
            key.mark();
            wrapper.contents = key;
        }

        @Override
        public Object getValue() throws IOException, InterruptedException {
            Object result = deserializer.deserializeKey(wrapper);
            key.reset();
            return result;
        }

        @Override
        public int compareTo(GroupInfo o) {
            return key.compareTo(((Group) o).key);
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + Objects.hashCode(key);
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            Group other = (Group) obj;
            if (!Objects.equals(key, other.key)) {
                return false;
            }
            return true;
        }
    }
}
