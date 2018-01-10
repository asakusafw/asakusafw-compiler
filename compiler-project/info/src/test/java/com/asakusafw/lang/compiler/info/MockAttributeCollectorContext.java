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
package com.asakusafw.lang.compiler.info;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import com.asakusafw.info.Attribute;
import com.asakusafw.lang.compiler.api.CompilerOptions;
import com.asakusafw.lang.compiler.common.Location;
import com.asakusafw.lang.compiler.model.graph.Batch;
import com.asakusafw.lang.compiler.model.graph.Jobflow;

/**
 * Mock {@link AttributeCollector.Context}.
 * @since 0.4.1
 */
public class MockAttributeCollectorContext implements AttributeCollector.Context {

    private CompilerOptions options = CompilerOptions.builder().build();

    private ClassLoader classLoader = getClass().getClassLoader();

    private final List<Attribute> attributes = new ArrayList<>();

    /**
     * Sets the options.
     * @param newValue the options
     * @return this
     */
    public MockAttributeCollectorContext withOptions(CompilerOptions newValue) {
        this.options = newValue;
        return this;
    }

    /**
     * Sets the class loader.
     * @param newValue the class loader
     * @return this
     */
    public MockAttributeCollectorContext withClassLoader(ClassLoader newValue) {
        this.classLoader = newValue;
        return this;
    }

    @Override
    public CompilerOptions getOptions() {
        return options;
    }

    @Override
    public ClassLoader getClassLoader() {
        return classLoader;
    }

    @Override
    public void putAttribute(Attribute attribute) {
        attributes.add(attribute);
    }

    @Override
    public InputStream findResourceFile(Location location) throws IOException {
        throw new IllegalStateException();
    }

    /**
     * Returns the added attributes.
     * @return the added attributes
     */
    public List<Attribute> getAttributes() {
        return attributes;
    }

    /**
     * Removes the added attributes.
     * @return this
     */
    public MockAttributeCollectorContext reset() {
        attributes.clear();
        return this;
    }

    /**
     * Collects attributes from the given batch model.
     * @param collector the collector
     * @param model the target model
     * @return the collected attributes
     */
    public List<Attribute> collect(Batch model, AttributeCollector collector) {
        return collect(() -> collector.process(this, model));
    }

    /**
     * Collects attributes from the given jobflow model.
     * @param collector the collector
     * @param model the target model
     * @return the collected attributes
     */
    public List<Attribute> collect(Jobflow model, AttributeCollector collector) {
        return collect(() -> collector.process(this, model));
    }

    private List<Attribute> collect(Runnable action) {
        attributes.clear();
        action.run();
        List<Attribute> results = new ArrayList<>(attributes);
        attributes.clear();
        return results;
    }
}
