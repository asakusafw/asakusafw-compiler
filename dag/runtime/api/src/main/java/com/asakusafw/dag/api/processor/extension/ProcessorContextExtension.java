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
package com.asakusafw.dag.api.processor.extension;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.ServiceLoader;

import com.asakusafw.dag.api.processor.ProcessorContext;
import com.asakusafw.lang.utils.common.Arguments;
import com.asakusafw.lang.utils.common.InterruptibleIo;
import com.asakusafw.lang.utils.common.InterruptibleIo.Closer;

/**
 * An extension point for {@link ProcessorContext}.
 * @since 0.4.0
 */
@FunctionalInterface
public interface ProcessorContextExtension {

    /**
     * Installs this extension.
     * @param context the target context
     * @param editor the editor for the target context
     * @return a disposer of installed object (nullable)
     * @throws IOException if I/O error was occurred while installing this extension
     * @throws InterruptedException if interrupted while installing this extension
     */
    InterruptibleIo install(
            ProcessorContext context,
            ProcessorContext.Editor editor) throws IOException, InterruptedException;

    /**
     * Loads {@link ProcessorContextExtension} objects via SPI.
     * @param classLoader the service class loader
     * @return a composite {@link ProcessorContextExtension} of loaded objects
     */
    static ProcessorContextExtension load(ClassLoader classLoader) {
        Arguments.requireNonNull(classLoader);
        List<ProcessorContextExtension> extensions = new ArrayList<>();
        for (ProcessorContextExtension extension : ServiceLoader.load(ProcessorContextExtension.class, classLoader)) {
            extensions.add(extension);
        }
        return new Composite(extensions);
    }

    /**
     * A composition of {@link ProcessorContextExtension}.
     * @since 0.4.0
     */
    class Composite implements ProcessorContextExtension {

        private final List<ProcessorContextExtension> elements;

        /**
         * Creates a new instance.
         * @param elements the elements
         */
        public Composite(Collection<? extends ProcessorContextExtension> elements) {
            Arguments.requireNonNull(elements);
            this.elements = Arguments.freezeToList(elements);
        }

        @Override
        public InterruptibleIo install(
                ProcessorContext context,
                ProcessorContext.Editor editor) throws IOException, InterruptedException {
            try (Closer closer = new Closer()) {
                for (ProcessorContextExtension extension : elements) {
                    InterruptibleIo result = extension.install(context, editor);
                    if (result != null) {
                        closer.add(result);
                    }
                }
                return closer.move();
            }
        }
    }
}
