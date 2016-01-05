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
package com.asakusafw.lang.compiler.hadoop;

import java.text.MessageFormat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asakusafw.lang.compiler.api.ExternalPortProcessor;
import com.asakusafw.lang.compiler.common.Diagnostic;
import com.asakusafw.lang.compiler.common.DiagnosticException;
import com.asakusafw.lang.compiler.common.ExtensionContainer;
import com.asakusafw.lang.compiler.model.info.ExternalInputInfo;

/**
 * Provides {@link InputFormatInfo} for accessing external inputs directly.
 */
public abstract class InputFormatInfoExtension {

    static final Logger LOG = LoggerFactory.getLogger(InputFormatInfoExtension.class);

    /**
     * Resolves an external input operator in this application, only if the input can be used via {@code InputFormat}.
     * @param extensions the extension container
     * @param name the target input name
     * @param info the {@link InputFormatInfo} for accessing the input,
     *   or {@code null} if the input cannot be used via {@code InputFormat}
     * @return the resolved information, or {@code null} if the target input does not support direct access
     * @throws DiagnosticException if error occurred while resolving the target external input
     */
    public static InputFormatInfo resolve(ExtensionContainer extensions, String name, ExternalInputInfo info) {
        InputFormatInfoExtension extension = extensions.getExtension(InputFormatInfoExtension.class);
        if (extension == null) {
            LOG.warn("InputFormat info support is not enabled");
            return null;
        }
        return extension.resolve(name, info);
    }

    /**
     * Resolves an external input operator in this application, only if the input can be used via {@code InputFormat}.
     * @param name the target input name
     * @param info the {@link InputFormatInfo} for accessing the input,
     *   or {@code null} if the input cannot be used via {@code InputFormat}
     * @return the resolved information, or {@code null} if the target input does not support direct access
     * @throws DiagnosticException if error occurred while resolving the target external input
     */
    public abstract InputFormatInfo resolve(String name, ExternalInputInfo info);

    /**
     * A default implementation of {@link InputFormatInfoExtension}.
     */
    public static class Basic extends InputFormatInfoExtension {

        private final ExternalPortProcessor.Context context;

        private final ExternalPortProcessor processor;

        /**
         * Creates a new instance.
         * @param context the current context
         * @param processor the current external input/output processor
         */
        public Basic(ExternalPortProcessor.Context context, ExternalPortProcessor processor) {
            this.context = context;
            this.processor = processor;
        }

        @Override
        public InputFormatInfo resolve(String name, ExternalInputInfo info) {
            Class<?> description = resolveClass(info);
            InputFormatInfoSupport support = processor.getAdaper(context, InputFormatInfoSupport.class, description);
            if (support == null) {
                return null;
            }
            return support.resolveInput(context, name, info);
        }

        private Class<?> resolveClass(ExternalInputInfo info) {
            try {
                return info.getDescriptionClass().resolve(context.getClassLoader());
            } catch (ClassNotFoundException e) {
                throw new DiagnosticException(Diagnostic.Level.ERROR, MessageFormat.format(
                        "failed to resolve a class: {0}",
                        info.getDescriptionClass().getClassName()), e);
            }
        }
    }
}
