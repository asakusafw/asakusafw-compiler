package com.asakusafw.lang.compiler.packaging;

import java.io.IOException;
import java.io.InputStream;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asakusafw.lang.compiler.common.Location;
import com.asakusafw.lang.compiler.common.Predicate;

/**
 * Assembles resources.
 */
public class ResourceAssembler {

    static final Logger LOG = LoggerFactory.getLogger(ResourceAssembler.class);

    static final Predicate<Object> ANY = new Predicate<Object>() {
        @Override
        public boolean apply(Object argument) {
            return true;
        }
    };

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
     * Assembles resources into the sink.
     * @param sink the target sink
     * @throws IOException if failed to assemble by I/O error
     */
    public void build(ResourceSink sink) throws IOException {
        List<RepositoryInfo> repos = new ArrayList<>();
        if (items.isEmpty() == false) {
            repos.add(new RepositoryInfo(new ResourceItemRepository(items.values())));
        }
        repos.addAll(repositories.values());
        Set<Location> saw = new HashSet<>();
        for (RepositoryInfo repo : repos) {
            copy(saw, repo, sink);
        }
    }

    private void copy(Set<Location> saw, RepositoryInfo repo, ResourceSink sink) throws IOException {
        Predicate<? super Location> predicate = repo.getAcceptor();
        try (ResourceRepository.Cursor cursor = repo.open()) {
            while (cursor.next()) {
                Location location = cursor.getLocation();
                if (saw.contains(location)) {
                    LOG.warn(MessageFormat.format(
                            "duplicate resource is ignored: {0} ({1})",
                            location,
                            repo));
                    continue;
                }
                saw.add(location);
                if (predicate.apply(location)) {
                    LOG.debug("include: {} ({})", location, repo);
                    try (InputStream contents = cursor.openResource()) {
                        sink.add(location, contents);
                    }
                } else {
                    LOG.debug("exclude: {} ({})", location, repo);
                }
            }
        }
    }

    private static class RepositoryInfo {

        private final ResourceRepository repository;

        private final Set<Predicate<? super Location>> predicates = new HashSet<>();

        public RepositoryInfo(ResourceRepository repository) {
            this.repository = repository;
        }

        void addPredicate(Predicate<? super Location> predicate) {
            predicates.add(predicate);
        }

        ResourceRepository.Cursor open() throws IOException {
            return repository.createCursor();
        }

        Predicate<? super Location> getAcceptor() {
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
                final List<Predicate<? super Location>> elements = new ArrayList<>(predicates);
                return new Predicate<Location>() {
                    @Override
                    public boolean apply(Location argument) {
                        for (int i = 0, n = elements.size(); i < n; i++) {
                            if (elements.get(i).apply(argument)) {
                                return true;
                            }
                        }
                        return false;
                    }
                };
            }
        }

        @Override
        public String toString() {
            return repository.toString();
        }
    }
}
