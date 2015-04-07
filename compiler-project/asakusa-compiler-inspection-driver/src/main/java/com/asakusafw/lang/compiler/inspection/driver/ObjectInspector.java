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
package com.asakusafw.lang.compiler.inspection.driver;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;

import com.asakusafw.lang.compiler.api.reference.BatchReference;
import com.asakusafw.lang.compiler.api.reference.JobflowReference;
import com.asakusafw.lang.compiler.common.Diagnostic;
import com.asakusafw.lang.compiler.common.DiagnosticException;
import com.asakusafw.lang.compiler.inspection.InspectionNode;
import com.asakusafw.lang.compiler.model.graph.Batch;
import com.asakusafw.lang.compiler.model.graph.Jobflow;
import com.asakusafw.lang.compiler.model.graph.OperatorGraph;
import com.asakusafw.lang.compiler.planning.Plan;

/**
 * Inspects internal compiler model objects.
 * <p>
 * Currently, this can inspect the following object types.
 * </p>
 * <ul>
 * <li> {@link Batch} </li>
 * <li> {@link Jobflow} </li>
 * <li> {@link OperatorGraph} </li>
 * <li> {@link BatchReference} </li>
 * <li> {@link JobflowReference} </li>
 * <li> {@link Plan} </li>
 * </ul>
 */
public class ObjectInspector {

    private static final List<Inspector<?>> INSPECTORS;
    static {
        List<Inspector<?>> list = new ArrayList<>();
        list.add(new Inspector<Batch>(Batch.class) {
            @Override
            public InspectionNode inspect(Batch object) {
                return new DslDriver().inspect(object);
            }
        });
        list.add(new Inspector<Jobflow>(Jobflow.class) {
            @Override
            public InspectionNode inspect(Jobflow object) {
                return new DslDriver().inspect(object);
            }
        });
        list.add(new Inspector<OperatorGraph>(OperatorGraph.class) {
            @Override
            public InspectionNode inspect(OperatorGraph object) {
                return new DslDriver().inspect("graph", object); //$NON-NLS-1$
            }
        });
        list.add(new Inspector<BatchReference>(BatchReference.class) {
            @Override
            public InspectionNode inspect(BatchReference object) {
                return new TaskDriver().inspect(object);
            }
        });
        list.add(new Inspector<JobflowReference>(JobflowReference.class) {
            @Override
            public InspectionNode inspect(JobflowReference object) {
                return new TaskDriver().inspect(object);
            }
        });
        list.add(new Inspector<Plan>(Plan.class) {
            @Override
            public InspectionNode inspect(Plan object) {
                return new PlanDriver().inspect("plan", object); //$NON-NLS-1$
            }
        });
        INSPECTORS = list;
    }

    /**
     * Returns whether this supports to inspect the target element.
     * @param element the target element
     * @return {@code true} if this supports the target element, otherwise {@code false}
     */
    public boolean isSupported(Object element) {
        Class<?> aClass = element.getClass();
        for (Inspector<?> inspector : INSPECTORS) {
            if (inspector.isSupported(aClass)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Inspects the target element.
     * @param element the target element
     * @return the inspection node
     * @throws DiagnosticException if failed to inspect the target element
     */
    public InspectionNode inspect(Object element) {
        Class<?> aClass = element.getClass();
        for (Inspector<?> inspector : INSPECTORS) {
            if (inspector.isSupported(aClass)) {
                return inspector.perform(element);
            }
        }
        throw new DiagnosticException(Diagnostic.Level.ERROR, MessageFormat.format(
                "inspection is not supported: {0}",
                element.getClass().getName()));
    }

    private abstract static class Inspector<T> {

        private final Class<T> supported;

        protected Inspector(Class<T> supported) {
            this.supported = supported;
        }

        public boolean isSupported(Class<?> target) {
            return supported.isAssignableFrom(target);
        }

        public InspectionNode perform(Object object) {
            return inspect(supported.cast(object));
        }

        public abstract InspectionNode inspect(T object);
    }
}
