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
package com.asakusafw.lang.compiler.extension.info;

import java.io.File;
import java.io.FileInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import com.asakusafw.info.Attribute;
import com.asakusafw.lang.compiler.api.CompilerOptions;
import com.asakusafw.lang.compiler.common.Location;
import com.asakusafw.lang.compiler.core.BatchCompiler;
import com.asakusafw.lang.compiler.core.basic.JobflowPackager;
import com.asakusafw.lang.compiler.info.AttributeCollector;
import com.asakusafw.lang.compiler.model.info.JobflowInfo;
import com.asakusafw.lang.compiler.packaging.FileContainer;

/**
 * An adapter of {@link com.asakusafw.lang.compiler.info.AttributeCollector.Context}.
 * @since 0.4.1
 */
class AttributeCollectorContextAdapter implements AttributeCollector.Context {

    private final BatchCompiler.Context delegate;

    private final List<Attribute> attributes = new ArrayList<>();

    private JobflowInfo current;

    /**
     * Creates a new instance.
     * @param delegate the delegate
     */
    AttributeCollectorContextAdapter(BatchCompiler.Context delegate) {
        this.delegate = delegate;
    }

    @Override
    public CompilerOptions getOptions() {
        return delegate.getOptions();
    }

    @Override
    public ClassLoader getClassLoader() {
        return delegate.getProject().getClassLoader();
    }

    @Override
    public void putAttribute(Attribute attribute) {
        attributes.add(attribute);
    }

    @Override
    public InputStream findResourceFile(Location location) throws IOException {
        if (current == null) {
            File file = delegate.getOutput().toFile(location);
            if (file.isFile()) {
                return new FileInputStream(file);
            } else {
                return null;
            }
        } else {
            FileContainer root = delegate.getOutput();
            File library = root.toFile(JobflowPackager.getLibraryLocation(current.getFlowId()));
            if (library.isFile() == false) {
                return null;
            }
            ZipFile zip = new ZipFile(library);
            boolean success = false;
            try {
                ZipEntry entry = zip.getEntry(location.toPath());
                if (entry == null) {
                    return null;
                }
                success = true;
                return new FilterInputStream(zip.getInputStream(entry)) {
                    @Override
                    public void close() throws IOException {
                        try {
                            super.close();
                        } finally {
                            zip.close();
                        }
                    }
                };
            } finally {
                if (success == false) {
                    zip.close();
                }
            }
        }
    }

    @Override
    public <T> T getExtension(Class<T> extension) {
        return delegate.getExtension(extension);
    }

    /**
     * Returns the batch attributes.
     * @return the batch attributes
     */
    public List<Attribute> getAttributes() {
        return attributes;
    }

    /**
     * Resets this context.
     * @param next the next target jobflow (nullable)
     */
    public void reset(JobflowInfo next) {
        this.current = next;
        attributes.clear();
    }
}
