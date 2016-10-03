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
package com.asakusafw.dag.api.processor.testing;

import java.util.Collection;
import java.util.Iterator;

import com.asakusafw.dag.api.processor.ObjectReader;
import com.asakusafw.lang.utils.common.Arguments;

/**
 * An {@link ObjectReader} implementation which reads objects from {@link Collection}.
 */
public class CollectionObjectReader implements ObjectReader {

    private final Iterator<?> objects;

    private Object current;

    /**
     * Creates a new instance.
     * @param objects the objects to read
     */
    public CollectionObjectReader(Collection<?> objects) {
        this.objects = objects.iterator();
    }

    @Override
    public boolean nextObject() {
        Arguments.requireNonNull(objects);
        if (objects.hasNext()) {
            current = objects.next();
            return true;
        } else {
            current = null;
            return false;
        }
    }

    @Override
    public Object getObject() {
        return current;
    }
}
