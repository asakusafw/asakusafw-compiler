/**
 * Copyright 2011-2021 Asakusa Framework Team.
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
package com.asakusafw.lang.compiler.api.testing;

import com.asakusafw.lang.compiler.api.DataModelLoader;
import com.asakusafw.lang.compiler.api.reference.DataModelReference;
import com.asakusafw.lang.compiler.model.description.Descriptions;
import com.asakusafw.lang.compiler.model.description.TypeDescription;
import com.asakusafw.runtime.model.DataModel;

/**
 * Mock implementation of {@link DataModelLoader}.
 * <p>
 * This loads each {@link DataModel} implementation, which provides {@code get<property-name>Option()} methods.
 * </p>
 */
public class MockDataModelLoader implements DataModelLoader {

    private final ClassLoader classLoader;

    /**
     * Creates a new instance.
     * @param classLoader the original class loader
     */
    public MockDataModelLoader(ClassLoader classLoader) {
        this.classLoader = classLoader;
    }

    @Override
    public DataModelReference load(TypeDescription type) {
        return MockDataModelProcessor.process(classLoader, type);
    }

    /**
     * Loads the data model type using {@link MockDataModelLoader}.
     * @param type the target type
     * @return the resolved reference
     */
    public static DataModelReference load(Class<?> type) {
        return load(type.getClassLoader(), Descriptions.typeOf(type));
    }

    /**
     * Loads the data model type using {@link MockDataModelLoader}.
     * @param classLoader the class loader for loading the data model type
     * @param type the target type
     * @return the resolved reference
     */
    public static DataModelReference load(ClassLoader classLoader, TypeDescription type) {
        return new MockDataModelLoader(classLoader).load(type);
    }
}
