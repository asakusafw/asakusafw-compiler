/**
 * Copyright 2011-2015 Asakusa Framework Team.
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
package com.asakusafw.lang.compiler.core.util;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.asakusafw.lang.compiler.api.DataModelProcessor;
import com.asakusafw.lang.compiler.api.reference.DataModelReference;
import com.asakusafw.lang.compiler.common.Diagnostic;
import com.asakusafw.lang.compiler.common.DiagnosticException;
import com.asakusafw.lang.compiler.model.description.TypeDescription;

/**
 * Composition of {@link DataModelProcessor}s.
 */
public class CompositeDataModelProcessor implements DataModelProcessor, CompositeElement<DataModelProcessor> {

    private final List<DataModelProcessor> elements;

    /**
     * Creates a new instance.
     * @param elements the element processors
     */
    public CompositeDataModelProcessor(List<? extends DataModelProcessor> elements) {
        this.elements = Collections.unmodifiableList(new ArrayList<>(elements));
    }

    @Override
    public List<DataModelProcessor> getElements() {
        return elements;
    }

    /**
     * Composites the element processors.
     * @param elements the element processors
     * @return the composite processor
     */
    public static DataModelProcessor composite(List<? extends DataModelProcessor> elements) {
        if (elements.size() == 1) {
            return elements.get(0);
        }
        return new CompositeDataModelProcessor(elements);
    }

    private DataModelProcessor getSupported(Context context, TypeDescription type) {
        for (DataModelProcessor element : elements) {
            if (element.isSupported(context, type)) {
                return element;
            }
        }
        return null;
    }

    @Override
    public boolean isSupported(Context context, TypeDescription type) {
        return getSupported(context, type) != null;
    }

    @Override
    public DataModelReference process(Context context, TypeDescription type) {
        DataModelProcessor element = getSupported(context, type);
        if (element == null) {
            throw new DiagnosticException(Diagnostic.Level.ERROR, MessageFormat.format(
                    "unsupported data model type: {0}",
                    type));
        }
        return element.process(context, type);
    }

    @Override
    public String toString() {
        if (elements.isEmpty()) {
            return "NULL"; //$NON-NLS-1$
        }
        return MessageFormat.format(
                "Composite{0}", //$NON-NLS-1$
                elements);
    }
}
