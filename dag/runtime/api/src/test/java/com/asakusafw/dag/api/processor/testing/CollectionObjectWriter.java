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

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import com.asakusafw.dag.api.processor.ObjectWriter;

/**
 * A {@link ObjectWriter} implementation which writes objects into {@link List} object.
 */
public class CollectionObjectWriter implements ObjectWriter {

    private final List<Object> results = new ArrayList<>();

    private final Function<Object, ?> function;

    /**
     * Creates a new instance.
     */
    public CollectionObjectWriter() {
        this(o -> o);
    }

    /**
     * Creates a new instance.
     * @param function a function to create result objects
     */
    public CollectionObjectWriter(Function<Object, ?> function) {
        this.function = function;
    }

    @Override
    public void putObject(Object object) {
        results.add(function.apply(object));
    }

    /**
     * Returns the results.
     * @return the results
     */
    public List<Object> getResults() {
        return results;
    }
}
