/**
 * Copyright 2011-2019 Asakusa Framework Team.
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
package com.asakusafw.lang.compiler.core.adapter;

import java.io.File;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import com.asakusafw.lang.compiler.api.BatchProcessor;
import com.asakusafw.lang.compiler.api.CompilerOptions;
import com.asakusafw.lang.compiler.api.reference.JobflowReference;
import com.asakusafw.lang.compiler.common.Location;
import com.asakusafw.lang.compiler.core.BatchCompiler;
import com.asakusafw.lang.compiler.core.basic.JobflowPackager;
import com.asakusafw.lang.compiler.packaging.FileContainer;

/**
 * An adapter for {@link BatchProcessor}.
 */
public class BatchProcessorAdapter implements BatchProcessor.Context {

    private final BatchCompiler.Context delegate;

    /**
     * Creates a new instance.
     * @param delegate the delegate target context
     */
    public BatchProcessorAdapter(BatchCompiler.Context delegate) {
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
    public OutputStream addResourceFile(Location location) throws IOException {
        return delegate.getOutput().addResource(location);
    }

    @Override
    public InputStream findResourceFile(JobflowReference jobflow, Location location) throws IOException {
        FileContainer root = delegate.getOutput();
        File library = root.toFile(JobflowPackager.getLibraryLocation(jobflow.getFlowId()));
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

    @Override
    public <T> T getExtension(Class<T> extension) {
        return delegate.getExtension(extension);
    }
}
