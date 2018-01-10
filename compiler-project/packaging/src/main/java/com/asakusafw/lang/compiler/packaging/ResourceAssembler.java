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
package com.asakusafw.lang.compiler.packaging;

import java.io.IOException;
import java.io.InputStream;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asakusafw.lang.compiler.common.Location;
import com.asakusafw.lang.compiler.common.Predicates;

/**
 * Assembles resources.
 */
public class ResourceAssembler {

    static final Logger LOG = LoggerFactory.getLogger(ResourceAssembler.class);

    static final Predicate<Object> ANY = Predicates.anything();

    private final Map<Location, ResourceItem> items = new LinkedHashMap<>();

    private final Map<ResourceRepository, RepositoryInfo> repositories = new HashMap<>();

    /**
     * Adds an item.
     * @param item the item
     * @return this
     */
    public ResourceAssembler addItem(ResourceItem item) {
        Location location = item.getLocation();
        if (items.containsKey(location)) {
            throw new IllegalArgumentException(MessageFormat.format(
                    "duplicate resource item location: {0}",
                    location));
        }
        items.put(location, item);
        return this;
    }

    /**
     * Adds all contents from the repository.
     * @param repository the resource repository
     * @return this
     * @see ResourceUtil
     */
    public ResourceAssembler addRepository(ResourceRepository repository) {
        return addRepository(repository, ANY);
    }

    /**
     * Adds selected contents from the repository.
     * @param repository the resource repository
     * @param acceptor contents filter
     * @return this
     * @see ResourceUtil
     */
    public ResourceAssembler addRepository(ResourceRepository repository, Predicate<? super Location> acceptor) {
        RepositoryInfo info = repositories.get(repository);
        if (info == null) {
            info = new RepositoryInfo(repository);
            repositories.put(repository, info);
        }
        info.addPredicate(acceptor);
        return this;
    }

    /**
     * Assembles resources and returns {@link ResourceRepository}.
     * @return the resource repository to obtain assembled results
     */
    public ResourceRepository build() {
        List<AssemblyRepository> assemblies = new ArrayList<>();
        if (items.isEmpty() == false) {
            assemblies.add(new AssemblyRepository(new ResourceItemRepository(new ArrayList<>(items.values())), ANY));
        }
        for (RepositoryInfo info : repositories.values()) {
            assemblies.add(info.toRepository());
        }
        if (assemblies.size() == 1) {
            return assemblies.get(0);
        }
        return new CompositeRepository(assemblies);
    }

    private static class RepositoryInfo {

        private final ResourceRepository repository;

        private final Set<Predicate<? super Location>> predicates = new HashSet<>();

        RepositoryInfo(ResourceRepository repository) {
            this.repository = repository;
        }

        void addPredicate(Predicate<? super Location> predicate) {
            predicates.add(predicate);
        }

        AssemblyRepository toRepository() {
            return new AssemblyRepository(repository, getAcceptor());
        }

        private Predicate<? super Location> getAcceptor() {
            if (predicates.isEmpty()) {
                return ANY;
            } else if (predicates.size() == 1) {
                return predicates.iterator().next();
            } else {
                for (Predicate<? super Location> p : predicates) {
                    if (p == ANY) {
                        return ANY;
                    }
                }
                List<Predicate<? super Location>> elements = new ArrayList<>(predicates);
                return location -> elements.stream().anyMatch(p -> p.test(location));
            }
        }

        @Override
        public String toString() {
            return repository.toString();
        }
    }

    private static class AssemblyRepository implements ResourceRepository {

        private final ResourceRepository repository;

        private final Predicate<? super Location> predicate;

        AssemblyRepository(ResourceRepository repository, Predicate<? super Location> predicate) {
            this.repository = repository;
            this.predicate = predicate;
        }

        @Override
        public AssemblyCusor createCursor() throws IOException {
            return new AssemblyCusor(repository, predicate);
        }

        @Override
        public String toString() {
            return repository.toString();
        }
    }

    private static class AssemblyCusor implements ResourceRepository.Cursor {

        private final ResourceRepository repository;

        private final ResourceRepository.Cursor cursor;

        private final Predicate<? super Location> predicate;

        AssemblyCusor(ResourceRepository repository, Predicate<? super Location> predicate) throws IOException {
            this.repository = repository;
            this.cursor = repository.createCursor();
            this.predicate = predicate;
        }

        ResourceRepository getRepository() {
            return repository;
        }

        @Override
        public boolean next() throws IOException {
            while (cursor.next()) {
                Location location = cursor.getLocation();
                if (predicate.test(location)) {
                    LOG.trace("include: {} ({})", location, repository); //$NON-NLS-1$
                    return true;
                } else {
                    LOG.trace("exclude: {} ({})", location, repository); //$NON-NLS-1$
                    continue;
                }
            }
            return false;
        }

        @Override
        public Location getLocation() {
            return cursor.getLocation();
        }

        @Override
        public InputStream openResource() throws IOException {
            return cursor.openResource();
        }

        @Override
        public void close() throws IOException {
            cursor.close();
        }
    }

    private static class CompositeRepository implements ResourceRepository {

        private final List<AssemblyRepository> elements;

        CompositeRepository(List<AssemblyRepository> elements) {
            this.elements = elements;
        }

        @Override
        public Cursor createCursor() throws IOException {
            return new CompositeCursor(elements.iterator());
        }
    }

    private static class CompositeCursor implements ResourceRepository.Cursor {

        private final Iterator<AssemblyRepository> repositories;

        private AssemblyCusor current;

        private final Set<Location> saw = new HashSet<>();

        CompositeCursor(Iterator<AssemblyRepository> repositories) {
            this.repositories = repositories;
        }

        @Override
        public boolean next() throws IOException {
            while (true) {
                if (current == null) {
                    if (repositories.hasNext() == false) {
                        return false;
                    }
                    current = repositories.next().createCursor();
                }
                if (current.next()) {
                    Location location = current.getLocation();
                    if (saw.contains(location)) {
                        LOG.warn(MessageFormat.format(
                                "ignore duplicated resource: {0} (in {1})",
                                location,
                                current.getRepository()));
                        continue;
                    }
                    saw.add(location);
                    return true;
                } else {
                    closeCurrent();
                }
            }
        }

        @Override
        public Location getLocation() {
            checkCurrent();
            return current.getLocation();
        }

        @Override
        public InputStream openResource() throws IOException {
            checkCurrent();
            return current.openResource();
        }

        private void checkCurrent() {
            if (current == null) {
                throw new IllegalStateException();
            }
        }

        @Override
        public void close() throws IOException {
            closeCurrent();
            while (repositories.hasNext()) {
                repositories.next();
            }
        }

        private void closeCurrent() throws IOException {
            if (current != null) {
                current.close();
                current = null;
            }
        }
    }
}
