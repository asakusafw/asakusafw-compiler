/**
 * Copyright 2011-2017 Asakusa Framework Team.
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
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;

import com.asakusafw.lang.compiler.common.Location;

/**
 * Composition of {@link ResourceRepository}.
 */
public class CompositeResourceRepository implements ResourceRepository {

    private final Set<ResourceRepository> repositories;

    /**
     * Creates a new instance.
     * @param repositories the element repositories
     */
    public CompositeResourceRepository(Collection< ? extends ResourceRepository> repositories) {
        this.repositories = new LinkedHashSet<>(repositories);
    }

    @Override
    public Cursor createCursor() throws IOException {
        return new CompositeCursor(repositories.iterator());
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + repositories.hashCode();
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
        CompositeResourceRepository other = (CompositeResourceRepository) obj;
        if (!repositories.equals(other.repositories)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return MessageFormat.format(
                "Composite{0}", //$NON-NLS-1$
                repositories);
    }

    private static final class CompositeCursor implements Cursor {

        private final Iterator<? extends ResourceRepository> repositories;

        private Cursor current;

        CompositeCursor(Iterator<? extends ResourceRepository> repositories) {
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
            // discards rest
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
