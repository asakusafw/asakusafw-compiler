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
package com.asakusafw.dag.runtime.io;

import java.util.Objects;

/**
 * Represents a tagged union record.
 * @since 0.4.0
 */
public final class UnionRecord {

    /**
     * The value tag.
     */
    public int tag;

    /**
     * The actual value.
     */
    public Object entity;

    /**
     * The next record.
     */
    public UnionRecord next;

    private UnionRecord nextCache;

    /**
     * Creates a new empty instance.
     */
    public UnionRecord() {
        return;
    }

    /**
     * Creates a new instance.
     * @param tag the value tag
     * @param entity the actual value
     */
    public UnionRecord(int tag, Object entity) {
        this(tag, entity, null);
    }

    /**
     * Creates a new instance.
     * @param tag the value tag
     * @param entity the actual value
     * @param next the next record (nullable)
     */
    public UnionRecord(int tag, Object entity, UnionRecord next) {
        this.tag = tag;
        this.entity = entity;
        this.next = next;
    }

    /**
     * Returns the value tag.
     * @return the value tag
     */
    public int getTag() {
        return tag;
    }

    /**
     * Returns the actual value.
     * @return the actual value
     */
    public Object getEntity() {
        return entity;
    }

    /**
     * Returns the next record.
     * @return the next record, or {@code null} if it does not exist
     */
    public UnionRecord getNext() {
        return next;
    }

    UnionRecord prepareNext() {
        UnionRecord cache = nextCache;
        if (cache == null) {
            cache = new UnionRecord();
            nextCache = cache;
        }
        next = cache;
        return cache;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + tag;
        result = prime * result + Objects.hashCode(entity);
        result = prime * result + Objects.hashCode(next);
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
        UnionRecord other = (UnionRecord) obj;
        if (tag != other.tag) {
            return false;
        }
        if (!Objects.equals(entity, other.entity)) {
            return false;
        }
        if (!Objects.equals(next, other.next)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder();
        buf.append('(');
        appendTo(buf);
        buf.append(')');
        return buf.toString();
    }

    private void appendTo(StringBuilder buf) {
        buf.append(tag);
        buf.append(':').append(entity);
        if (next != null) {
            buf.append(", ");
            next.appendTo(buf);
        }
    }
}
