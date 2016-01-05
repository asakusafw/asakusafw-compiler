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
package com.asakusafw.lang.compiler.packaging;

import java.io.IOException;
import java.io.InputStream;
import java.text.MessageFormat;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;

import com.asakusafw.lang.compiler.common.Location;

/**
 * An implementation of {@link ResourceRepository} using set of {@link ResourceItem}s.
 */
public class ResourceItemRepository implements ResourceRepository {

    private final Set<ResourceItem> items;

    /**
     * Creates a new instance.
     * @param items the resource items
     */
    public ResourceItemRepository(Collection<? extends ResourceItem> items) {
        this(items, true);
    }

    /**
     * Creates a new instance.
     * @param items the resource items
     * @param failOnConflict {@code true} to fail if locations are conflict between items, otherwise {@code false}
     */
    public ResourceItemRepository(Collection<? extends ResourceItem> items, boolean failOnConflict) {
        this.items = new LinkedHashSet<>(items);
        if (failOnConflict) {
            Set<Location> saw = new HashSet<>();
            for (ResourceItem item : items) {
                Location location = item.getLocation();
                if (saw.contains(location)) {
                    throw new IllegalArgumentException(MessageFormat.format(
                            "duplicated item location: {0}",
                            location));
                }
                saw.add(location);
            }
        }
    }

    @Override
    public Cursor createCursor() throws IOException {
        return new ItemCursor(items.iterator());
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + items.hashCode();
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
        ResourceItemRepository other = (ResourceItemRepository) obj;
        if (!items.equals(other.items)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return MessageFormat.format(
                "Items{0}", //$NON-NLS-1$
                items);
    }

    static class ItemCursor implements Cursor {

        private final Iterator<? extends ResourceItem> iterator;

        private ResourceItem current;

        ItemCursor(Iterator<? extends ResourceItem> iterator) {
            assert iterator != null;
            this.iterator = iterator;
        }

        @Override
        public boolean next() throws IOException {
            if (iterator.hasNext() == false) {
                return false;
            }
            current = iterator.next();
            return true;
        }

        @Override
        public Location getLocation() {
            return current.getLocation();
        }

        @Override
        public InputStream openResource() throws IOException {
            return current.openResource();
        }

        @Override
        public void close() throws IOException {
            // do nothing
        }
    }
}
