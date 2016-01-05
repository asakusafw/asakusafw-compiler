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
package com.asakusafw.lang.compiler.inspection;

import java.text.MessageFormat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asakusafw.lang.compiler.common.DiagnosticException;
import com.asakusafw.lang.compiler.common.ExtensionContainer;
import com.asakusafw.lang.compiler.common.Location;

/**
 * A compiler extension for inspecting support.
 */
public abstract class InspectionExtension {

    static final Logger LOG = LoggerFactory.getLogger(InspectionExtension.class);

    /**
     * Inspects the target element only if inspection is supported in this session.
     * @param extensions the extension container
     * @param location the output location of inspection information
     * @param element the target element
     * @throws DiagnosticException if failed to inspect the target element
     */
    public static void inspect(ExtensionContainer extensions, Location location, Object element) {
        InspectionExtension extension = extensions.getExtension(InspectionExtension.class);
        if (extension == null) {
            LOG.warn("inspection support is not enabled");
            return;
        }
        if (extension.isSupported(element) == false) {
            LOG.warn(MessageFormat.format(
                    "this session does not support inspection: {0}",
                    element.getClass().getName()));
            return;
        }
        extension.inspect(location, element);
    }

    /**
     * Returns whether this supports to inspect the target element.
     * @param element the target element
     * @return {@code true} if this supports the target element, otherwise {@code false}
     */
    public abstract boolean isSupported(Object element);

    /**
     * Inspects the target element.
     * @param location the output location of inspection information
     * @param element the target element
     * @throws DiagnosticException if failed to inspect the target element
     */
    public abstract void inspect(Location location, Object element);
}
