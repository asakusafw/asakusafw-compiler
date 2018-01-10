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
package com.asakusafw.lang.compiler.core.util;

import java.lang.ref.Reference;
import java.lang.ref.SoftReference;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.WeakHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asakusafw.lang.compiler.common.Diagnostic;
import com.asakusafw.lang.compiler.common.DiagnosticException;
import com.asakusafw.lang.compiler.core.ClassAnalyzer;
import com.asakusafw.lang.compiler.model.graph.Batch;
import com.asakusafw.lang.compiler.model.graph.Jobflow;
import com.asakusafw.lang.compiler.model.graph.OperatorGraph;

/**
 * Composition of {@link ClassAnalyzer}.
 * This loads element {@link ClassAnalyzer}s via SPI
 * (on {@code META-INF/services/com.asakusafw.lang.compiler.core.ClassAnalyzer}).
 * @since 0.3.0
 */
public class CompositeClassAnalyzer implements ClassAnalyzer {

    static final Logger LOG = LoggerFactory.getLogger(CompositeClassAnalyzer.class);

    private final Map<ClassLoader, Reference<ClassAnalyzer[]>> cacheMap = new WeakHashMap<>();

    private ClassAnalyzer[] getElements(Context context) {
        ClassLoader loader = context.getProject().getClassLoader();
        Reference<ClassAnalyzer[]> cacheRef;
        synchronized (cacheMap) {
            cacheRef = cacheMap.get(loader);
        }
        ClassAnalyzer[] cached = cacheRef == null ? null : cacheRef.get();
        if (cached != null) {
            return cached;
        }
        List<ClassAnalyzer> loaded = new ArrayList<>();
        for (ClassAnalyzer element : ServiceLoader.load(ClassAnalyzer.class, loader)) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("loaded class analyzer: {}", element);
            }
            loaded.add(element);
        }
        ClassAnalyzer[] results = loaded.toArray(new ClassAnalyzer[loaded.size()]);
        synchronized (cacheMap) {
            cacheMap.put(loader, new SoftReference<>(results));
        }
        return results;
    }

    @Override
    public boolean isBatchClass(Context context, Class<?> aClass) {
        for (ClassAnalyzer element : getElements(context)) {
            if (element.isBatchClass(context, aClass)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean isJobflowClass(Context context, Class<?> aClass) {
        for (ClassAnalyzer element : getElements(context)) {
            if (element.isJobflowClass(context, aClass)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean isFlowObject(Context context, Object object) {
        for (ClassAnalyzer element : getElements(context)) {
            if (element.isFlowObject(context, object)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public Batch analyzeBatch(Context context, Class<?> batchClass) {
        for (ClassAnalyzer element : getElements(context)) {
            if (element.isBatchClass(context, batchClass)) {
                return element.analyzeBatch(context, batchClass);
            }
        }
        throw new DiagnosticException(Diagnostic.Level.ERROR, MessageFormat.format(
                "unsupported batch class: {0}",
                batchClass.getName()));
    }

    @Override
    public Jobflow analyzeJobflow(Context context, Class<?> jobflowClass) {
        for (ClassAnalyzer element : getElements(context)) {
            if (element.isJobflowClass(context, jobflowClass)) {
                return element.analyzeJobflow(context, jobflowClass);
            }
        }
        throw new DiagnosticException(Diagnostic.Level.ERROR, MessageFormat.format(
                "unsupported jobflow class: {0}",
                jobflowClass.getName()));
    }

    @Override
    public OperatorGraph analyzeFlow(Context context, Object flowObject) {
        for (ClassAnalyzer element : getElements(context)) {
            if (element.isFlowObject(context, flowObject)) {
                return element.analyzeFlow(context, flowObject);
            }
        }
        throw new DiagnosticException(Diagnostic.Level.ERROR, MessageFormat.format(
                "unsupported flow oject: {0}",
                flowObject));
    }

    @Override
    public String toString() {
        return "CompositeClassAnalyzer"; //$NON-NLS-1$
    }
}
