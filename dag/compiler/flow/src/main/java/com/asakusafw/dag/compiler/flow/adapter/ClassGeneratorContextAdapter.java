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
package com.asakusafw.dag.compiler.flow.adapter;

import java.io.IOException;
import java.io.OutputStream;
import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import com.asakusafw.dag.compiler.codegen.ClassGeneratorContext;
import com.asakusafw.dag.compiler.codegen.ClassNameMap;
import com.asakusafw.dag.compiler.model.ClassData;
import com.asakusafw.lang.compiler.api.DataModelLoader;
import com.asakusafw.lang.compiler.api.JobflowProcessor;
import com.asakusafw.lang.compiler.common.Diagnostic;
import com.asakusafw.lang.compiler.common.DiagnosticException;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.utils.common.Arguments;
import com.asakusafw.lang.utils.common.Invariants;
import com.asakusafw.lang.utils.common.Optionals;

/**
 * An adapter implementation of {@link ClassGeneratorContext}.
 * @since 0.4.0
 */
public class ClassGeneratorContextAdapter implements ClassGeneratorContext {

    private final JobflowProcessor.Context delegate;

    private final ClassNameMap namer;

    private final Map<Object, ClassDescription> cache = new HashMap<>();

    /**
     * Creates a new instance.
     * @param delegate the parent context
     * @param classNamePrefix the prefix of fully qualified class names to generate
     */
    public ClassGeneratorContextAdapter(JobflowProcessor.Context delegate, String classNamePrefix) {
        Arguments.requireNonNull(delegate);
        Arguments.requireNonNull(classNamePrefix);
        this.delegate = delegate;
        this.namer = new ClassNameMap(classNamePrefix);
    }

    @Override
    public ClassLoader getClassLoader() {
        return delegate.getClassLoader();
    }

    @Override
    public DataModelLoader getDataModelLoader() {
        return delegate.getDataModelLoader();
    }

    @Override
    public ClassDescription getClassName(String category, String hint) {
        return namer.get(category, hint);
    }

    @Override
    public ClassDescription addClassFile(ClassData data) {
        if (data.hasContents()) {
            try (OutputStream output = delegate.addClassFile(data.getDescription())) {
                data.dump(output);
            } catch (IOException e) {
                throw new DiagnosticException(Diagnostic.Level.ERROR, MessageFormat.format(
                        "error occurred while generating a class file: {0}",
                        data.getDescription().getBinaryName()), e);
            }
        }
        return data.getDescription();
    }

    @Override
    public Optional<ClassDescription> findCache(Object key) {
        return Optionals.get(cache, key);
    }

    @Override
    public void addCache(Object key, ClassDescription target) {
        ClassDescription victim = cache.putIfAbsent(key, target);
        Invariants.require(victim == null, () -> key);
    }
}
