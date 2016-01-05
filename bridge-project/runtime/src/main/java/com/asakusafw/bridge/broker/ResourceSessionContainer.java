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
package com.asakusafw.bridge.broker;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.Iterator;
import java.util.Map;
import java.util.WeakHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asakusafw.bridge.broker.ResourceBroker.Initializer;
import com.asakusafw.bridge.broker.ResourceBroker.Scope;

final class ResourceSessionContainer {

    static final Logger LOG = LoggerFactory.getLogger(ResourceSessionContainer.class);

    private Store store = newStore(ResourceBroker.DEFAULT_SCOPE);

    /**
     * Creates a new session.
     * @param scope the session scope
     * @return the created session, or {@code null} if this already has another session in the scope
     * @throws IllegalStateException if there are active sessions with different scope
     */
    public synchronized ResourceSessionEntity.Reference create(Scope scope) {
        Store s = prepare(scope);
        ResourceSessionEntity conflict = s.find();
        if (conflict != null) {
            return null;
        }
        ResourceSessionEntity entity = new ResourceSessionEntity();
        ResourceSessionEntity.Reference result = entity.newReference();
        s.put(entity);
        return result;
    }

    /**
     * Creates a new session and returns the its first reference.
     * @param scope the session scope
     * @param initializer the session initializer
     * @param orGet {@code true} to return a reference of existing session
     * @return the created session, or {@code null} if this already has another session in the scope
     * @throws IOException if error occurred while initializing the new session
     * @throws IllegalStateException if there are active sessions with different scope
     */
    public synchronized ResourceSessionEntity.Reference create(
            Scope scope, Initializer initializer, boolean orGet) throws IOException {
        Store s = prepare(scope);
        ResourceSessionEntity conflict = s.find();
        if (conflict == null) {
            ResourceSessionEntity entity = new ResourceSessionEntity();
            boolean success = false;
            try {
                initializer.accept(entity);
                success = true;
            } finally {
                if (success == false) {
                    entity.close();
                }
            }
            ResourceSessionEntity.Reference result = entity.newReference();
            s.put(entity);
            return result;
        } else if (orGet) {
            synchronized (conflict) {
                if (conflict.closed == false) {
                    return conflict.newReference();
                }
            }
            // retry
            return create(scope, initializer, orGet);
        } else {
            return null;
        }
    }

    /**
     * Returns the current session.
     * @return the current session, or {@code null} if it is not found
     * @throws IllegalStateException if there is no current session
     */
    public synchronized ResourceSessionEntity find() {
        return store.find();
    }

    /**
     * Disposes all active sessions.
     */
    public synchronized void dispose() {
        if (store != null) {
            store.close();
        }
    }

    private Store prepare(Scope scope) {
        if (scope == store.getScope()) {
            return store;
        } else if (store.isEmpty()) {
            LOG.debug("creating session container: {}", scope);
            Store s = newStore(scope);
            this.store = s;
            return s;
        } else {
            throw new IllegalStateException(MessageFormat.format(
                    "another session already exists with different scope: {0} <=> {1}",
                    scope,
                    store.getScope()));
        }
    }

    private static Store newStore(Scope scope) {
        switch (scope) {
        case VM:
            return new VmStore();
        case THREAD:
            return new ThreadStore();
        default:
            throw new AssertionError(scope);
        }
    }

    private interface Store {

        Scope getScope();

        boolean isEmpty();

        void put(ResourceSessionEntity value);

        ResourceSessionEntity find();

        void close();
    }

    private static class VmStore implements Store {

        private ResourceSessionEntity entity;

        VmStore() {
            return;
        }

        @Override
        public Scope getScope() {
            return Scope.VM;
        }

        @Override
        public boolean isEmpty() {
            return entity == null || entity.closed;
        }

        @Override
        public void put(ResourceSessionEntity value) {
            entity = value;
        }

        @Override
        public ResourceSessionEntity find() {
            return isEmpty() ? null : entity;
        }

        @Override
        public void close() {
            if (entity != null) {
                entity.close();
            }
        }
    }

    private static class ThreadStore implements Store {

        private final Map<Thread, ResourceSessionEntity> entities = new WeakHashMap<>();

        ThreadStore() {
            return;
        }

        @Override
        public Scope getScope() {
            return Scope.THREAD;
        }

        @Override
        public boolean isEmpty() {
            cleanup();
            return entities.isEmpty();
        }

        private void cleanup() {
            if (entities.isEmpty()) {
                return;
            }
            for (Iterator<Map.Entry<Thread, ResourceSessionEntity>> iter = entities.entrySet().iterator();
                    iter.hasNext();) {
                ResourceSessionEntity entity = iter.next().getValue();
                if (entity.closed) {
                    iter.remove();
                }
            }
        }

        @Override
        public void put(ResourceSessionEntity value) {
            entities.put(Thread.currentThread(), value);
        }

        @Override
        public ResourceSessionEntity find() {
            ResourceSessionEntity entity = entities.get(Thread.currentThread());
            return entity == null || entity.closed ? null : entity;
        }

        @Override
        public void close() {
            for (ResourceSessionEntity entity : entities.values()) {
                entity.close();
            }
        }
    }
}
