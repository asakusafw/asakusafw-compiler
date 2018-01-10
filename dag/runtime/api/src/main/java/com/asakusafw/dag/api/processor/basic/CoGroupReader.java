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
package com.asakusafw.dag.api.processor.basic;

import java.io.IOException;
import java.util.List;
import java.util.stream.Stream;

import com.asakusafw.dag.api.common.ObjectCursor;
import com.asakusafw.dag.api.processor.EdgeReader;
import com.asakusafw.dag.api.processor.GroupReader;
import com.asakusafw.dag.api.processor.GroupReader.GroupInfo;
import com.asakusafw.lang.utils.common.Arguments;
import com.asakusafw.lang.utils.common.Lang;

/**
 * Bundles {@link GroupReader} and build co-group sequence.
 * @since 0.4.0
 */
public final class CoGroupReader implements EdgeReader {

    private static final ObjectCursor EMPTY = new ObjectCursor() {
        @Override
        public boolean nextObject() throws IOException, InterruptedException {
            return false;
        }
        @Override
        public Object getObject() throws IOException, InterruptedException {
            throw new IllegalStateException();
        }
    };

    private final Element[] elements;

    /**
     * Creates a new instance.
     * @param elements readers to read each group
     */
    public CoGroupReader(List<? extends GroupReader> elements) {
        Arguments.requireNonNull(elements);
        Arguments.require(elements.size() > 0);
        this.elements = elements.stream().map(Element::new).toArray(Element[]::new);
    }

    /**
     * Creates a new instance.
     * @param elements readers to read each group
     */
    public CoGroupReader(GroupReader... elements) {
        Arguments.requireNonNull(elements);
        Arguments.require(elements.length > 0);
        this.elements = Stream.of(elements).map(Element::new).toArray(Element[]::new);
    }

    /**
     * Returns the number of groups.
     * @return the number of groups
     */
    public int getGroupCount() {
        return elements.length;
    }

    /**
     * Advances the cursor in the next co-group, and returns whether or not the next co-group exists.
     * This method may change previously {@link #getGroup(int)} result objects.
     * @throws IOException if I/O error occurred while reading the next co-group
     * @throws InterruptedException if interrupted while reading the next co-group
     * @return {@code true} if the next group exists, otherwise {@code false}
     */
    public boolean nextCoGroup() throws IOException, InterruptedException {
        Element[] es = elements;
        int minIndex = -1;
        GroupInfo minKey = null;
        boolean found = false;
        for (int index = 0, n = es.length; index < n; index++) {
            Element element = es[index];
            GroupInfo key = element.next();
            if (key == null) {
                // no more groups in the target element
                continue;
            }
            if (minKey == null) {
                // the first element's key is always the left-most minimum
                element.head = true;
                minIndex = index;
                minKey = key;
                found = true;
            } else {
                int diff = minKey.compareTo(key);
                if (diff < 0) {
                    // the key is NOT minimum
                    element.head = false;
                } else if (diff == 0) {
                    // the key is equivalent to the minimum one but it is NOT the left-most
                    element.head = true;
                } else {
                    // the key is the left-most minimum
                    element.head = true;

                    // the left-most key was changed; retract the old left-most key info
                    for (int j = minIndex; j < index; j++) {
                        es[j].head = false;
                    }
                    minIndex = index;
                    minKey = key;
                }
            }
        }
        return found;
    }

    /**
     * Returns an object cursor to read from the co-group element.
     * @param index the group index (0-origin)
     * @return the object cursor for the target group
     */
    public ObjectCursor getGroup(int index) {
        Element element = elements[index];
        if (element.head) {
            return element.reader;
        } else {
            return EMPTY;
        }
    }

    @Override
    public void close() throws IOException, InterruptedException {
        Exception occurred = null;
        for (Element element : elements) {
            try {
                element.reader.close();
            } catch (IOException | InterruptedException e) {
                if (occurred == null) {
                    occurred = e;
                } else {
                    occurred.addSuppressed(e);
                }
            }
        }
        if (occurred != null) {
            Lang.rethrow(occurred, IOException.class);
            Lang.rethrow(occurred, InterruptedException.class);
            throw new IllegalStateException(occurred);
        }
    }

    private static final class Element {

        final GroupReader reader;

        private GroupInfo key;

        boolean head;

        private boolean sawEof;

        Element(GroupReader reader) {
            this.reader = reader;
            this.head = true;
        }

        GroupInfo next() throws IOException, InterruptedException {
            if (sawEof) {
                return null;
            }
            if (head == false) {
                assert key != null;
                return key;
            }
            if (reader.nextGroup()) {
                key = reader.getGroup();
                return key;
            } else {
                key = null;
                sawEof = true;
                head = false;
                return null;
            }
        }
    }
}
