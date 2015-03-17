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

import java.io.File;
import java.io.IOException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.asakusafw.lang.compiler.common.Location;

/**
 * A builder for resource packages.
 */
public class FilePackageBuilder {

    private final List<ResourceRepository> repositories = new ArrayList<>();

    private final Map<Location, ContentProvider> providers = new LinkedHashMap<>();

    private final List<FileVisitor> visitors = new ArrayList<>();

    /**
     * Returns whether this package is empty or not.
     * @return {@code true} if this package is empty, otherwise {@code false}
     */
    public boolean isEmpty() {
        return repositories.isEmpty() && providers.isEmpty() && visitors.isEmpty();
    }

    /**
     * Adds resources in the repository.
     * @param repository the source repository
     * @return this
     */
    public FilePackageBuilder add(ResourceRepository repository) {
        this.repositories.add(repository);
        return this;
    }

    /**
     * Adds a resource item.
     * @param item the resource item
     * @return this
     */
    public FilePackageBuilder add(ResourceItem item) {
        this.providers.put(item.getLocation(), item);
        return this;
    }

    /**
     * Adds a resource using the content provider.
     * @param location the target location (relative from the package root)
     * @param provider the content provider
     * @return this
     */
    public FilePackageBuilder add(Location location, ContentProvider provider) {
        this.providers.put(location, provider);
        return this;
    }

    /**
     * Adds a content visitor.
     * This will be performed after package contents were deployed.
     * Clients may use visitors for customizing deployed files (e.g. changing file permissions).
     * @param visitor the content visitor
     * @return this
     */
    public FilePackageBuilder add(FileVisitor visitor) {
        this.visitors.add(visitor);
        return this;
    }

    /**
     * Builds a package into the destination path.
     * @param destination the destination path
     * @throws IOException if failed to build the package
     */
    public void build(File destination) throws IOException {
        if (destination.mkdirs() == false && destination.isDirectory() == false) {
            throw new IOException(MessageFormat.format(
                    "failed to create package: {0}",
                    destination));
        }
        if (isEmpty()) {
            return;
        }
        FileContainer container = new FileContainer(destination);
        try (ResourceSink sink = container.createSink()) {
            for (ResourceRepository repository : repositories) {
                ResourceUtil.copy(repository, sink);
            }
            for (Map.Entry<Location, ContentProvider> entry : providers.entrySet()) {
                Location location = entry.getKey();
                final ContentProvider provider = entry.getValue();
                sink.add(location, provider);
            }
        }
        for (FileVisitor visitor : visitors) {
            ResourceUtil.visit(visitor, destination);
        }
    }
}
