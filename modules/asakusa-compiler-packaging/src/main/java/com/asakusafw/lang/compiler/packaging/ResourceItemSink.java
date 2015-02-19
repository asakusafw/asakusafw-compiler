/**
 * Copyright 2011-2015 Asakusa Framework Team.
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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

import com.asakusafw.lang.compiler.common.Location;

/**
 * An implementation of {@link ResourceSink} which keeps added resources as {@link ResourceItem}.
 */
public class ResourceItemSink implements ResourceSink {

    private final Map<Location, ByteArrayItem> items = new LinkedHashMap<>();

    /**
     * Returns the all added items.
     * @return the all added items
     */
    public Collection<ByteArrayItem> getItems() {
        return new ArrayList<>(items.values());
    }

    /**
     * Returns an added item.
     * @param location the resource location
     * @return the related item, or {@code null} if there are no such an item
     */
    public ByteArrayItem find(Location location) {
        return items.get(location);
    }

    @Override
    public void add(Location location, InputStream contents) throws IOException {
        ByteArrayItem item = ResourceUtil.toItem(location, contents);
        items.put(location, item);
    }

    @Override
    public void add(Location location, Callback callback) throws IOException {
        try (ByteArrayOutputStream output = new ByteArrayOutputStream()) {
            callback.add(location, output);
            items.put(location, new ByteArrayItem(location, output.toByteArray()));
        }
    }

    @Override
    public void close() throws IOException {
        return;
    }
}
