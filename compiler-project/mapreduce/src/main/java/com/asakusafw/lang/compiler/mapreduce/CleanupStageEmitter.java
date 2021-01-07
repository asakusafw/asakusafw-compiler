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
package com.asakusafw.lang.compiler.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.asakusafw.lang.compiler.javac.JavaSourceExtension;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.runtime.stage.AbstractCleanupStageClient;
import com.asakusafw.runtime.stage.BaseStageClient;
import com.asakusafw.utils.java.model.syntax.CompilationUnit;
import com.asakusafw.utils.java.model.syntax.Literal;
import com.asakusafw.utils.java.model.syntax.MethodDeclaration;
import com.asakusafw.utils.java.model.syntax.ModelFactory;
import com.asakusafw.utils.java.model.syntax.TypeBodyDeclaration;
import com.asakusafw.utils.java.model.syntax.TypeDeclaration;
import com.asakusafw.utils.java.model.util.AttributeBuilder;
import com.asakusafw.utils.java.model.util.ImportBuilder;
import com.asakusafw.utils.java.model.util.Models;

/**
 * Emits a stage client for copying datasets.
 * @see CleanupStageInfo
 */
public final class CleanupStageEmitter {

    private final ModelFactory f = Models.getModelFactory();

    private final ClassDescription clientClass;

    private final CleanupStageInfo info;

    private final ImportBuilder importer;

    private CleanupStageEmitter(ClassDescription clientClass, CleanupStageInfo info) {
        this.clientClass = clientClass;
        this.info = info;
        this.importer = JavaDomUtil.newImportBuilder(clientClass);
    }

    /**
     * Emits a MapReduce stage client class.
     * @param clientClass the client class name
     * @param info the client class info
     * @param javac the target Java compiler
     * @throws IOException if failed to emit the class by I/O error
     */
    public static void emit(
            ClassDescription clientClass,
            CleanupStageInfo info,
            JavaSourceExtension javac) throws IOException {
        CompilationUnit unit = new CleanupStageEmitter(clientClass, info).generate();
        JavaDomUtil.emit(unit, javac);
    }

    private CompilationUnit generate() {
        TypeDeclaration type = generateType();
        return f.newCompilationUnit(
                importer.getPackageDeclaration(),
                importer.toImportDeclarations(),
                Collections.singletonList(type),
                Collections.emptyList());
    }

    private TypeDeclaration generateType() {
        List<TypeBodyDeclaration> members = new ArrayList<>();
        members.addAll(convertMeta(info.meta));
        members.addAll(convertCleanupPath(info.cleanupPath));
        return f.newClassDeclaration(
                null,
                new AttributeBuilder(f)
                    .Public()
                    .toAttributes(),
                f.newSimpleName(clientClass.getSimpleName()),
                importer.toType(AbstractCleanupStageClient.class),
                Collections.emptyList(),
                members);
    }

    private List<MethodDeclaration> convertMeta(StageInfo meta) {
        List<MethodDeclaration> results = new ArrayList<>();
        results.add(newGetter(BaseStageClient.METHOD_BATCH_ID, meta.getBatchId()));
        results.add(newGetter(BaseStageClient.METHOD_FLOW_ID, meta.getFlowId()));
        results.add(newGetter(BaseStageClient.METHOD_STAGE_ID, meta.getStageId()));
        return results;
    }

    private List<MethodDeclaration> convertCleanupPath(String baseOutputPath) {
        return Collections.singletonList(newGetter(AbstractCleanupStageClient.METHOD_CLEANUP_PATH, baseOutputPath));
    }

    private MethodDeclaration newGetter(String name, String value) {
        Literal literal = Models.toLiteral(f, value);
        return f.newMethodDeclaration(
                null,
                new AttributeBuilder(f)
                    .annotation(importer.toType(Override.class))
                    .Public()
                    .toAttributes(),
                importer.toType(String.class),
                f.newSimpleName(name),
                Collections.emptyList(),
                Collections.singletonList(f.newReturnStatement(literal)));
    }
}
