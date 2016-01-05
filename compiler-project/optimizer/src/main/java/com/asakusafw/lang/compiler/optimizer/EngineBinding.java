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
package com.asakusafw.lang.compiler.optimizer;

import java.util.Collection;
import java.util.Collections;

import com.asakusafw.lang.compiler.model.description.ClassDescription;

/**
 * Represents bindings between engine and its target operators.
 * @param <T> the engine type
 * @since 0.1.0
 * @version 0.3.0
 */
public interface EngineBinding<T> {

    /**
     * Returns the target operator annotation types.
     * @return the target operator annotation types
     */
    Collection<ClassDescription> getTargetOperators();

    /**
     * Returns the target custom operator categories.
     * @return the category tags
     */
    Collection<String> getTargetCategories();

    /**
     * Returns the target external input module name.
     * @return the target external input module name
     */
    Collection<String> getTargetInputs();

    /**
     * Returns the target external output module name.
     * @return the target external output module name
     */
    Collection<String> getTargetOutputs();

    /**
     * Returns an engine for the target operators.
     * @return an engine
     */
    T getEngine();

    /**
     * An abstract implementation of {@link EngineBinding}.
     * @param <T> the engine type
     */
    public abstract class Abstract<T> implements EngineBinding<T> {

        @Override
        public Collection<ClassDescription> getTargetOperators() {
            return Collections.emptySet();
        }

        @Override
        public Collection<String> getTargetCategories() {
            return Collections.emptySet();
        }

        @Override
        public Collection<String> getTargetInputs() {
            return Collections.emptySet();
        }

        @Override
        public Collection<String> getTargetOutputs() {
            return Collections.emptySet();
        }
    }
}
