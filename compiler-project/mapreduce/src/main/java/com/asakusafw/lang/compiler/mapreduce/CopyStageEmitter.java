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
package com.asakusafw.lang.compiler.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.io.NullWritable;

import com.asakusafw.lang.compiler.javac.JavaSourceExtension;
import com.asakusafw.lang.compiler.mapreduce.CopyStageInfo.Operation;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.description.Descriptions;
import com.asakusafw.runtime.stage.preparator.PreparatorMapper;
import com.asakusafw.utils.java.model.syntax.Comment;
import com.asakusafw.utils.java.model.syntax.CompilationUnit;
import com.asakusafw.utils.java.model.syntax.FormalParameterDeclaration;
import com.asakusafw.utils.java.model.syntax.MethodDeclaration;
import com.asakusafw.utils.java.model.syntax.ModelFactory;
import com.asakusafw.utils.java.model.syntax.Type;
import com.asakusafw.utils.java.model.syntax.TypeBodyDeclaration;
import com.asakusafw.utils.java.model.syntax.TypeDeclaration;
import com.asakusafw.utils.java.model.util.AttributeBuilder;
import com.asakusafw.utils.java.model.util.ImportBuilder;
import com.asakusafw.utils.java.model.util.Models;

/**
 * Emits a stage client for copying datasets.
 * @see CopyStageInfo
 */
public final class CopyStageEmitter {

    private final CopyStageInfo info;

    private CopyStageEmitter(CopyStageInfo info) {
        this.info = info;
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
            CopyStageInfo info,
            JavaSourceExtension javac) throws IOException {
        CopyStageEmitter emitter = new CopyStageEmitter(info);
        MapReduceStageInfo generic = emitter.generateMappers(clientClass, javac);
        MapReduceStageEmitter.emit(clientClass, generic, javac);
    }

    private MapReduceStageInfo generateMappers(
            ClassDescription clientClass,
            JavaSourceExtension javac) throws IOException {
        List<MapReduceStageInfo.Input> inputs = new ArrayList<>();
        List<MapReduceStageInfo.Output> outputs = new ArrayList<>();
        int index = 0;
        for (Operation operation : info.operations) {
            ClassDescription mapperClass = new ClassDescription(String.format(
                    "%s.Mapper%d", //$NON-NLS-1$
                    clientClass.getPackageName(),
                    index++));
            CompilationUnit unit = MapperGenerator.generate(info, operation, mapperClass);
            JavaDomUtil.emit(unit, javac);
            SourceInfo source = operation.source;
            for (String path : source.getPaths()) {
                inputs.add(new MapReduceStageInfo.Input(
                        path,
                        source.getDataClass(),
                        source.getFormatClass(),
                        mapperClass,
                        source.getAttributes()));
            }
            outputs.add(new MapReduceStageInfo.Output(
                    operation.outputName,
                    Descriptions.classOf(NullWritable.class),
                    source.getDataClass(),
                    operation.outputFormatClass,
                    operation.outputAttributes));
        }
        return new MapReduceStageInfo(
                info.meta,
                inputs,
                outputs,
                Collections.<MapReduceStageInfo.Resource>emptyList(),
                info.baseOutputPath);
    }

    private static final class MapperGenerator {

        private final ModelFactory f = Models.getModelFactory();

        private final CopyStageInfo.Operation operation;

        private final ClassDescription mapperClass;

        private final ImportBuilder importer;

        private MapperGenerator(
                CopyStageInfo info,
                CopyStageInfo.Operation operation,
                ClassDescription mapperClass) {
            this.operation = operation;
            this.mapperClass = mapperClass;
            this.importer = JavaDomUtil.newImportBuilder(mapperClass);
        }

        static CompilationUnit generate(
                CopyStageInfo info,
                CopyStageInfo.Operation operation,
                ClassDescription mapperClass) {
            return new MapperGenerator(info, operation, mapperClass).generate();
        }

        private CompilationUnit generate() {
            TypeDeclaration type = generateType();
            return f.newCompilationUnit(
                    importer.getPackageDeclaration(),
                    importer.toImportDeclarations(),
                    Collections.singletonList(type),
                    Collections.<Comment>emptyList());
        }

        private TypeDeclaration generateType() {
            List<TypeBodyDeclaration> members = new ArrayList<>();
            members.add(generateMethod());
            return f.newClassDeclaration(
                    null,
                    new AttributeBuilder(f)
                        .Public()
                        .toAttributes(),
                    f.newSimpleName(mapperClass.getSimpleName()),
                    f.newParameterizedType(
                            importer.toType(PreparatorMapper.class),
                            importer.toType(Object.class)),
                    Collections.<Type>emptyList(),
                    members);
        }

        private MethodDeclaration generateMethod() {
            return f.newMethodDeclaration(
                    null,
                    new AttributeBuilder(f)
                        .annotation(importer.toType(Override.class))
                        .Public()
                        .toAttributes(),
                    importer.toType(String.class),
                    f.newSimpleName(PreparatorMapper.NAME_GET_OUTPUT_NAME),
                    Collections.<FormalParameterDeclaration>emptyList(),
                    Collections.singletonList(f.newReturnStatement(Models.toLiteral(f, operation.outputName))));
        }
    }
}
