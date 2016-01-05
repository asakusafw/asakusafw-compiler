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

import com.asakusafw.lang.compiler.common.DiagnosticException;
import com.asakusafw.lang.inspection.InspectionNode;

/**
 * Inspects the internal compiler model objects.
 */
public interface ObjectInspector {

    /**
     * Returns whether this supports to inspect the target element.
     * @param element the target element
     * @return {@code true} if this supports the target element, otherwise {@code false}
     */
    boolean isSupported(Object element);

    /**
     * Inspects the target element.
     * @param element the target element
     * @return the inspection node
     * @throws DiagnosticException if failed to inspect the target element
     */
    InspectionNode inspect(Object element);

}