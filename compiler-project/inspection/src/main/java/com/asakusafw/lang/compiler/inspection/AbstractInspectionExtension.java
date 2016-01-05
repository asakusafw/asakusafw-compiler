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

import java.io.IOException;
import java.io.OutputStream;
import java.text.MessageFormat;

import com.asakusafw.lang.compiler.common.Diagnostic;
import com.asakusafw.lang.compiler.common.DiagnosticException;
import com.asakusafw.lang.compiler.common.Location;
import com.asakusafw.lang.compiler.common.ResourceContainer;
import com.asakusafw.lang.inspection.InspectionNode;
import com.asakusafw.lang.inspection.InspectionNodeRepository;
import com.asakusafw.lang.inspection.json.JsonInspectionNodeRepository;

/**
 * An abstract implementation of {@link InspectionExtension}.
 */
public abstract class AbstractInspectionExtension extends InspectionExtension implements ResourceContainer {

    private final ObjectInspector inspector;

    private final InspectionNodeRepository repository;

    /**
     * Creates a new instance with default settings.
     */
    public AbstractInspectionExtension() {
        this(new BasicObjectInspector(), new JsonInspectionNodeRepository());
    }

    /**
     * Creates a new instance.
     * @param inspector the inspector
     * @param repository the repository
     */
    public AbstractInspectionExtension(ObjectInspector inspector, InspectionNodeRepository repository) {
        this.inspector = inspector;
        this.repository = repository;
    }

    @Override
    public boolean isSupported(Object element) {
        return inspector.isSupported(element);
    }

    @Override
    public void inspect(Location location, Object element) {
        InspectionNode node = inspector.inspect(element);
        try (OutputStream output = addResource(location)) {
            repository.store(output, node);
        } catch (IOException e) {
            throw new DiagnosticException(Diagnostic.Level.ERROR, MessageFormat.format(
                    "failed to save inspection object: {0}",
                    node), e);
        }
    }
}
