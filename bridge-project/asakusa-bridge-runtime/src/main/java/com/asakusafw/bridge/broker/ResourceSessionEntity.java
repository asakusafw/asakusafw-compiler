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
package com.asakusafw.bridge.broker;

import java.io.Closeable;
import java.io.IOException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.Callable;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class ResourceSessionEntity implements ResourceSession {

    static final Logger LOG = LoggerFactory.getLogger(ResourceSessionEntity.class);

    private final Set<Reference> references = new LinkedHashSet<>();

    private final Map<Class<?>, Object> resources = new HashMap<>();

    private final Map<Class<?>, ReadWriteLock> resourceLocks = new WeakHashMap<>();

    volatile boolean closed = false;

    public ResourceSessionEntity() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("start session: {}", toString());
        }
    }

    @Override
    public <T> T find(Class<T> type) {
        ReadWriteLock rwLock = getResourceLock(type);
        Lock lock = rwLock.readLock();
        lock.lock();
        try {
            Object value = resources.get(type);
            if (value == null) {
                return null;
            }
            return type.cast(value);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public <T> void put(Class<T> type, T resource) {
        ReadWriteLock rwLock = getResourceLock(type);
        Lock lock = rwLock.readLock();
        lock.lock();
        try {
            if (resources.containsKey(type)) {
                throw new IllegalStateException(MessageFormat.format(
                        "target resource is already exists in this session: {0}",
                        type.getName()));
            }
            resources.put(type, resource);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public <T> T get(Class<T> type) {
        T found = find(type);
        if (found != null) {
            return found;
        }
        throw new NoSuchElementException(type.getName());
    }

    @Override
    public <T> T get(Class<T> type, Callable<? extends T> supplier) {
        ReadWriteLock rwLock = getResourceLock(type);
        Lock lock = rwLock.writeLock();
        lock.lock();
        try {
            Object value = resources.get(type);
            if (value != null) {
                return type.cast(value);
            }
            T result = supplier.call();
            resources.put(type, result);
            return result;
        } catch (Exception e) {
            throw new IllegalStateException(MessageFormat.format(
                    "failed to supply a resource: {0}",
                    type.getName()), e);
        } finally {
            lock.unlock();
        }
    }

    private ReadWriteLock getResourceLock(Class<?> type) {
        synchronized (resourceLocks) {
            ReadWriteLock lock = resourceLocks.get(type);
            if (lock == null) {
                lock = new ReentrantReadWriteLock();
                resourceLocks.put(type, lock);
            }
            return lock;
        }
    }

    synchronized ResourceSession newReference() {
        if (closed) {
            throw new IllegalStateException();
        }
        Reference reference = new Reference();
        LOG.debug("create session reference: {}", reference);
        references.add(reference);
        return reference;
    }

    synchronized void close(Reference reference) {
        LOG.debug("close session reference: {}", reference);
        references.remove(reference);
        if (references.isEmpty()) {
            close();
        }
    }

    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        LOG.debug("close session: {}", this);
        references.clear();
        List<Object> values = new ArrayList<>(resources.values());
        resources.clear();
        for (Object object : values) {
            if (object instanceof Closeable) {
                try {
                    ((Closeable) object).close();
                } catch (IOException e) {
                    LOG.warn(MessageFormat.format(
                            "failed to close a registered resource: {0}",
                            object), e);
                }
            }
        }
        resourceLocks.clear();
        closed = true;
    }

    private final class Reference implements ResourceSession {

        Reference() {
            return;
        }

        @Override
        public <T> T find(Class<T> type) {
            return ResourceSessionEntity.this.find(type);
        }

        @Override
        public <T> T get(Class<T> type) {
            return ResourceSessionEntity.this.get(type);
        }

        @Override
        public <T> T get(Class<T> type, Callable<? extends T> supplier) {
            return ResourceSessionEntity.this.get(type, supplier);
        }

        @Override
        public <T> void put(Class<T> type, T resource) {
            ResourceSessionEntity.this.put(type, resource);
        }

        @Override
        public void close() {
            ResourceSessionEntity.this.close(this);
        }
    }
}
