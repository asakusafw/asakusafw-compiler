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
package com.asakusafw.lang.compiler.extension.directio.emitter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.NullWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asakusafw.lang.compiler.api.reference.DataModelReference;
import com.asakusafw.lang.compiler.extension.directio.DirectFileIoConstants;
import com.asakusafw.lang.compiler.javac.JavaSourceExtension;
import com.asakusafw.lang.compiler.mapreduce.JavaDomUtil;
import com.asakusafw.lang.compiler.mapreduce.MapReduceStageEmitter;
import com.asakusafw.lang.compiler.mapreduce.MapReduceStageInfo;
import com.asakusafw.lang.compiler.mapreduce.SourceInfo;
import com.asakusafw.lang.compiler.mapreduce.StageInfo;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.description.Descriptions;
import com.asakusafw.runtime.directio.DirectDataSourceConstants;
import com.asakusafw.runtime.directio.FilePattern;
import com.asakusafw.runtime.io.util.ShuffleKey;
import com.asakusafw.runtime.io.util.ShuffleKey.AbstractGroupComparator;
import com.asakusafw.runtime.io.util.ShuffleKey.AbstractOrderComparator;
import com.asakusafw.runtime.stage.directio.AbstractDirectOutputKey;
import com.asakusafw.runtime.stage.directio.AbstractDirectOutputMapper;
import com.asakusafw.runtime.stage.directio.AbstractDirectOutputValue;
import com.asakusafw.runtime.stage.directio.AbstractNoReduceDirectOutputMapper;
import com.asakusafw.runtime.stage.directio.DirectOutputReducer;
import com.asakusafw.runtime.stage.directio.DirectOutputSpec;
import com.asakusafw.utils.java.model.syntax.ArrayType;
import com.asakusafw.utils.java.model.syntax.ClassDeclaration;
import com.asakusafw.utils.java.model.syntax.Comment;
import com.asakusafw.utils.java.model.syntax.CompilationUnit;
import com.asakusafw.utils.java.model.syntax.ConstructorDeclaration;
import com.asakusafw.utils.java.model.syntax.Expression;
import com.asakusafw.utils.java.model.syntax.FormalParameterDeclaration;
import com.asakusafw.utils.java.model.syntax.ModelFactory;
import com.asakusafw.utils.java.model.syntax.Statement;
import com.asakusafw.utils.java.model.syntax.Type;
import com.asakusafw.utils.java.model.util.AttributeBuilder;
import com.asakusafw.utils.java.model.util.ImportBuilder;
import com.asakusafw.utils.java.model.util.Models;
import com.asakusafw.utils.java.model.util.TypeBuilder;

/**
 * Emits a stage class for direct output.
 */
public final class OutputStageEmitter {

    static final Logger LOG = LoggerFactory.getLogger(OutputStageEmitter.class);

    private final OutputStageInfo stageInfo;

    private final JavaSourceExtension javac;

    private final Map<OutputStageInfo.Operation, ClassDescription> stringTemplateClasses = new LinkedHashMap<>();
    private final Map<OutputStageInfo.Operation, ClassDescription> orderingClasses = new LinkedHashMap<>();

    private OutputStageEmitter(OutputStageInfo stageInfo, JavaSourceExtension javac) {
        this.javac = javac;
        this.stageInfo = stageInfo;
    }

    /**
     * Emits the stage client class and its dependencies.
     * @param stageInfo the target stage information
     * @param javac the java compiler
     * @return the created client class
     * @throws IOException if failed to emit classes
     */
    public static ClassDescription emit(OutputStageInfo stageInfo, JavaSourceExtension javac) throws IOException {
        OutputStageEmitter emitter = new OutputStageEmitter(stageInfo, javac);
        return emitter.emit();
    }

    private ClassDescription emit() throws IOException {
        if (requiresReducer()) {
            return emitClientWithReducer();
        } else {
            return emitClientWithoutReducer();
        }
    }

    private boolean requiresReducer() {
        for (OutputStageInfo.Operation output : stageInfo.operations) {
            if (output.resourcePattern.isGatherRequired()) {
                return true;
            }
        }
        return false;
    }

    private ClassDescription emitClientWithReducer() throws IOException {
        LOG.debug("Emitting shuffle key for Direct I/O epilogue"); //$NON-NLS-1$
        ClassDescription key = emitKey();

        LOG.debug("Emitting shuffle value for Direct I/O epilogue"); //$NON-NLS-1$
        ClassDescription value = emitValue();

        LOG.debug("Emitting grouping comparator for Direct I/O epilogue"); //$NON-NLS-1$
        ClassDescription grouping = emitGrouping(key);

        LOG.debug("Emitting sort comparator for Direct I/O epilogue"); //$NON-NLS-1$
        ClassDescription ordering = emitOrdering(key);

        LOG.debug("Emitting mappers for Direct I/O epilogue"); //$NON-NLS-1$
        List<CompiledOperation> compiledOperations = emitMappers(key, value);

        LOG.debug("Emitting stage client (with reducer) for Direct I/O epilogue"); //$NON-NLS-1$
        ClassDescription client = emitClient(
                compiledOperations,
                new MapReduceStageInfo.Shuffle(
                        key, value,
                        Descriptions.classOf(ShuffleKey.Partitioner.class), null,
                        ordering, grouping,
                        Descriptions.classOf(DirectOutputReducer.class)));

        LOG.debug("Finish preparing output stage for Direct I/O epilogue: " //$NON-NLS-1$
                + "batch={}, flow={}, class={}", new Object[] { //$NON-NLS-1$
                stageInfo.meta.getBatchId(),
                stageInfo.meta.getFlowId(),
                client.getName(),
        });
        return client;
    }

    private ClassDescription emitClientWithoutReducer() throws IOException {
        LOG.debug("Emitting mappers for Direct I/O epilogue"); //$NON-NLS-1$
        List<CompiledOperation> compiledOperations = emitMappers(null, null);

        LOG.debug("Emitting stage client (without reducer) for Direct I/O epilogue"); //$NON-NLS-1$
        ClassDescription client = emitClient(compiledOperations, null);

        LOG.debug("Finish preparing output stage for Direct I/O epilogue: " //$NON-NLS-1$
                + "batch={}, flow={}, class={}", new Object[] { //$NON-NLS-1$
                stageInfo.meta.getBatchId(),
                stageInfo.meta.getFlowId(),
                client.getName(),
        });
        return client;
    }

    private ClassDescription emitKey() throws IOException {
        return emitWithSpecs(
                stageInfo.meta.getStageClass("ShuffleKey"), //$NON-NLS-1$
                AbstractDirectOutputKey.class);
    }

    private ClassDescription emitValue() throws IOException {
        return emitWithSpecs(
                stageInfo.meta.getStageClass("ShuffleValue"), //$NON-NLS-1$
                AbstractDirectOutputValue.class);
    }

    private ClassDescription emitGrouping(ClassDescription key) throws IOException {
        assert key != null;
        return emitWithClass(
                stageInfo.meta.getStageClass("ShuffleGroupingComparator"), //$NON-NLS-1$
                AbstractGroupComparator.class,
                key);
    }

    private ClassDescription emitOrdering(ClassDescription key) throws IOException {
        return emitWithClass(
                stageInfo.meta.getStageClass("ShuffleSortComparator"), //$NON-NLS-1$
                AbstractOrderComparator.class,
                key);
    }

    private List<CompiledOperation> emitMappers(
            ClassDescription keyOrNull, ClassDescription valueOrNull) throws IOException {
        List<CompiledOperation> results = new ArrayList<>();
        int index = 0;
        for (OutputStageInfo.Operation operation : stageInfo.operations) {
            ClassDescription mapper;
            if (operation.resourcePattern.isGatherRequired()) {
                assert keyOrNull != null;
                assert valueOrNull != null;
                mapper = emitShuffleMapper(operation, index, keyOrNull, valueOrNull);
            } else {
                mapper = emitOutputMapper(operation, index);
            }
            results.add(new CompiledOperation(operation, mapper));
            index++;
        }
        return results;
    }

    private ClassDescription emitShuffleMapper(
            OutputStageInfo.Operation operation, int index,
            ClassDescription key, ClassDescription value) throws IOException {
        assert operation != null;
        assert key != null;
        assert value != null;
        assert index >= 0;
        assert operation.resourcePattern.isGatherRequired();
        ModelFactory f = Models.getModelFactory();
        ClassDescription aClass = stageInfo.meta.getStageClass("Mapper", index); //$NON-NLS-1$
        ImportBuilder importer = JavaDomUtil.newImportBuilder(aClass);
        importer.resolvePackageMember(f.newSimpleName(aClass.getSimpleName()));
        List<Expression> arguments = new ArrayList<>();
        arguments.add(Models.toLiteral(f, index));
        arguments.add(classLiteralOrNull(f, importer, key));
        arguments.add(classLiteralOrNull(f, importer, value));
        return emitConstructorClass(
                aClass,
                f.newParameterizedType(
                        importer.toType(AbstractDirectOutputMapper.class),
                        resolve(importer, operation.dataModel)),
                importer,
                arguments);
    }

    private ClassDescription emitOutputMapper(OutputStageInfo.Operation operation, int index) throws IOException {
        assert operation != null;
        assert index >= 0;
        assert operation.resourcePattern.isContextRequired();
        ModelFactory f = Models.getModelFactory();
        ClassDescription aClass = stageInfo.meta.getStageClass("Mapper", index); //$NON-NLS-1$
        ImportBuilder importer = JavaDomUtil.newImportBuilder(aClass);
        importer.resolvePackageMember(f.newSimpleName(aClass.getSimpleName()));
        List<Expression> arguments = new ArrayList<>();
        arguments.add(f.newClassLiteral(resolve(importer, operation.dataModel)));
        arguments.add(Models.toLiteral(f, operation.basePath));
        arguments.add(Models.toLiteral(f, operation.resourcePattern.getResourcePatternString()));
        arguments.add(f.newClassLiteral(resolve(importer, operation.dataFormatClass)));
        return emitConstructorClass(
                aClass,
                f.newParameterizedType(
                        importer.toType(AbstractNoReduceDirectOutputMapper.class),
                        resolve(importer, operation.dataModel)),
                importer,
                arguments);
    }

    private ClassDescription emitWithSpecs(ClassDescription aClass, Class<?> baseClass) throws IOException {
        assert aClass != null;
        assert baseClass != null;
        ModelFactory f = Models.getModelFactory();
        ImportBuilder importer = JavaDomUtil.newImportBuilder(aClass);
        importer.resolvePackageMember(f.newSimpleName(aClass.getSimpleName()));
        List<Expression> elements = new ArrayList<>();
        for (OutputStageInfo.Operation operation : stageInfo.operations) {
            if (operation.resourcePattern.isGatherRequired()) {
                ClassDescription stringTemplateClass = getStringTemplateClass(operation);
                ClassDescription orderingClass = getOrderingClass(operation);
                List<Expression> arguments = new ArrayList<>();
                arguments.add(f.newClassLiteral(resolve(importer, operation.dataModel)));
                arguments.add(Models.toLiteral(f, operation.basePath));
                arguments.add(f.newClassLiteral(resolve(importer, operation.dataFormatClass)));
                arguments.add(f.newClassLiteral(resolve(importer, stringTemplateClass)));
                arguments.add(f.newClassLiteral(resolve(importer, orderingClass)));
                elements.add(new TypeBuilder(f, importer.toType(DirectOutputSpec.class))
                    .newObject(arguments)
                    .toExpression());
            } else {
                elements.add(Models.toNullLiteral(f));
            }
        }
        return emitConstructorClass(
                aClass,
                importer.toType(baseClass),
                importer,
                Collections.singletonList(f.newArrayCreationExpression(
                        (ArrayType) importer.toType(DirectOutputSpec[].class),
                        f.newArrayInitializer(elements))));
    }

    private ClassDescription getStringTemplateClass(OutputStageInfo.Operation operation) throws IOException {
        Map<OutputStageInfo.Operation, ClassDescription> classes = stringTemplateClasses;
        ClassDescription generated = classes.get(operation);
        if (generated != null) {
            return generated;
        }
        int index = classes.size() + 1;
        ClassDescription targetClass = stageInfo.meta.getStageClass("Naming", index); //$NON-NLS-1$
        StringTemplateClassEmitter.emit(targetClass, operation, javac);
        classes.put(operation, targetClass);
        return targetClass;
    }

    private ClassDescription getOrderingClass(OutputStageInfo.Operation operation) throws IOException {
        Map<OutputStageInfo.Operation, ClassDescription> classes = orderingClasses;
        ClassDescription generated = classes.get(operation);
        if (generated != null) {
            return generated;
        }
        int index = classes.size() + 1;
        ClassDescription targetClass = stageInfo.meta.getStageClass("Ordering", index); //$NON-NLS-1$
        OrderingClassEmitter.emit(targetClass, operation, javac);
        classes.put(operation, targetClass);
        return targetClass;
    }

    private Expression classLiteralOrNull(ModelFactory f, ImportBuilder importer, ClassDescription nameOrNull) {
        assert f != null;
        assert importer != null;
        if (nameOrNull == null) {
            return Models.toNullLiteral(f);
        } else {
            return f.newClassLiteral(resolve(importer, nameOrNull));
        }
    }

    static Type resolve(ImportBuilder importer, DataModelReference dataType) {
        return importer.toType(JavaDomUtil.getName(dataType.getDeclaration()));
    }

    static Type resolve(ImportBuilder importer, ClassDescription aClass) {
        return importer.toType(JavaDomUtil.getName(aClass));
    }

    private ClassDescription emitWithClass(
            ClassDescription aClass,
            Class<?> baseClass,
            ClassDescription argumentClassName) throws IOException {
        assert aClass != null;
        assert baseClass != null;
        assert argumentClassName != null;
        ModelFactory f = Models.getModelFactory();
        ImportBuilder importer = JavaDomUtil.newImportBuilder(aClass);
        importer.resolvePackageMember(f.newSimpleName(aClass.getSimpleName()));
        List<Expression> arguments = new ArrayList<>();
        arguments.add(classLiteralOrNull(f, importer, argumentClassName));
        return emitConstructorClass(aClass, importer.toType(baseClass), importer, arguments);
    }

    private ClassDescription emitConstructorClass(
            ClassDescription aClass,
            Type baseClass,
            ImportBuilder importer,
            List<? extends Expression> arguments) throws IOException {
        assert importer != null;
        assert arguments != null;
        ModelFactory f = Models.getModelFactory();
        Statement ctorChain = f.newSuperConstructorInvocation(arguments);
        ConstructorDeclaration ctorDecl = f.newConstructorDeclaration(
                null,
                new AttributeBuilder(f)
                    .Public()
                    .toAttributes(),
                f.newSimpleName(aClass.getSimpleName()),
                Collections.<FormalParameterDeclaration>emptyList(),
                Collections.singletonList(ctorChain));
        ClassDeclaration typeDecl = f.newClassDeclaration(
                null,
                new AttributeBuilder(f)
                    .Public()
                    .Final()
                    .toAttributes(),
                f.newSimpleName(aClass.getSimpleName()),
                importer.resolve(baseClass),
                Collections.<Type>emptyList(),
                Collections.singletonList(ctorDecl));
        CompilationUnit source = f.newCompilationUnit(
                importer.getPackageDeclaration(),
                importer.toImportDeclarations(),
                Collections.singletonList(typeDecl),
                Collections.<Comment>emptyList());
        return JavaDomUtil.emit(source, javac);
    }

    private ClassDescription emitClient(
            List<CompiledOperation> compiledOperations,
            MapReduceStageInfo.Shuffle shuffle) throws IOException {
        assert compiledOperations != null;
        List<MapReduceStageInfo.Input> inputs = new ArrayList<>();
        List<MapReduceStageInfo.Output> outputs = new ArrayList<>();
        for (CompiledOperation compiled : compiledOperations) {
            OutputStageInfo.Operation operation = compiled.original;
            for (SourceInfo source : operation.sources) {
                for (String path : source.getPaths()) {
                    inputs.add(new MapReduceStageInfo.Input(
                            path,
                            source.getDataClass(),
                            source.getFormatClass(),
                            compiled.mapperClass,
                            source.getAttributes()));
                }
            }
            Map<String, String> outputAttributes = new LinkedHashMap<>();
            int index = 0;
            for (FilePattern pattern : operation.deletePatterns) {
                outputAttributes.put(
                        String.format("%s%02d", //$NON-NLS-1$
                                DirectDataSourceConstants.PREFIX_DELETE_PATTERN, index++),
                        pattern.getPatternString());
            }
            outputs.add(new MapReduceStageInfo.Output(
                    operation.basePath,
                    Descriptions.classOf(NullWritable.class),
                    operation.dataModel.getDeclaration(),
                    DirectFileIoConstants.CLASS_OUTPUT_FORMAT,
                    outputAttributes));
        }

        MapReduceStageInfo info = new MapReduceStageInfo(
                new StageInfo(stageInfo.meta.getBatchId(), stageInfo.meta.getFlowId(), stageInfo.meta.getStageId()),
                inputs,
                outputs,
                Collections.<MapReduceStageInfo.Resource>emptyList(),
                shuffle,
                stageInfo.baseOutputPath);

        ClassDescription aClass = stageInfo.meta.getStageClass("StageClient"); //$NON-NLS-1$
        MapReduceStageEmitter.emit(aClass, info, javac);
        return aClass;
    }

    private static class CompiledOperation {

        final OutputStageInfo.Operation original;

        final ClassDescription mapperClass;

        CompiledOperation(OutputStageInfo.Operation original, ClassDescription mapperClass) {
            this.original = original;
            this.mapperClass = mapperClass;
        }
    }
}
