package com.asakusafw.lang.compiler.mapreduce;

import static com.asakusafw.runtime.stage.AbstractStageClient.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.asakusafw.lang.compiler.javac.JavaSourceExtension;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.runtime.stage.AbstractStageClient;
import com.asakusafw.runtime.stage.BaseStageClient;
import com.asakusafw.runtime.stage.StageInput;
import com.asakusafw.runtime.stage.StageOutput;
import com.asakusafw.runtime.stage.StageResource;
import com.asakusafw.utils.java.model.syntax.ClassLiteral;
import com.asakusafw.utils.java.model.syntax.Comment;
import com.asakusafw.utils.java.model.syntax.CompilationUnit;
import com.asakusafw.utils.java.model.syntax.Expression;
import com.asakusafw.utils.java.model.syntax.FormalParameterDeclaration;
import com.asakusafw.utils.java.model.syntax.Literal;
import com.asakusafw.utils.java.model.syntax.MethodDeclaration;
import com.asakusafw.utils.java.model.syntax.ModelFactory;
import com.asakusafw.utils.java.model.syntax.SimpleName;
import com.asakusafw.utils.java.model.syntax.Statement;
import com.asakusafw.utils.java.model.syntax.Type;
import com.asakusafw.utils.java.model.syntax.TypeBodyDeclaration;
import com.asakusafw.utils.java.model.syntax.TypeDeclaration;
import com.asakusafw.utils.java.model.util.AttributeBuilder;
import com.asakusafw.utils.java.model.util.ExpressionBuilder;
import com.asakusafw.utils.java.model.util.ImportBuilder;
import com.asakusafw.utils.java.model.util.Models;
import com.asakusafw.utils.java.model.util.TypeBuilder;

/**
 * Emits MapReduce stage client classes.
 */
public final class MapReduceStageEmitter {

    private final ModelFactory f = Models.getModelFactory();

    private final ClassDescription clientClass;

    private final MapReduceStageInfo info;

    private final ImportBuilder importer;

    private MapReduceStageEmitter(ClassDescription clientClass, MapReduceStageInfo info) {
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
            MapReduceStageInfo info,
            JavaSourceExtension javac) throws IOException {
        CompilationUnit unit = new MapReduceStageEmitter(clientClass, info).generate();
        JavaDomUtil.emit(unit, javac);
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
        members.addAll(convertMeta(info.meta));
        members.addAll(convertInputs(info.inputs));
        members.addAll(convertOutputs(info.outputs));
        members.addAll(convertResources(info.resources));
        members.addAll(convertShuffle(info.shuffle));
        members.addAll(convertBaseOutput(info.baseOutputPath));
        return f.newClassDeclaration(
                null,
                new AttributeBuilder(f)
                    .Public()
                    .toAttributes(),
                f.newSimpleName(clientClass.getSimpleName()),
                importer.toType(AbstractStageClient.class),
                Collections.<Type>emptyList(),
                members);
    }

    private List<MethodDeclaration> convertMeta(StageInfo meta) {
        List<MethodDeclaration> results = new ArrayList<>();
        results.add(newGetter(BaseStageClient.METHOD_BATCH_ID, String.class, literal(meta.getBatchId())));
        results.add(newGetter(BaseStageClient.METHOD_FLOW_ID, String.class, literal(meta.getFlowId())));
        results.add(newGetter(BaseStageClient.METHOD_STAGE_ID, String.class, literal(meta.getStageId())));
        return results;
    }

    private List<MethodDeclaration> convertInputs(List<MapReduceStageInfo.Input> inputs) {
        List<Statement> statements = new ArrayList<>();
        if (inputs.isEmpty()) {
            statements.add(new TypeBuilder(f, importer.toType(Collections.class))
                    .method("emptyList") //$NON-NLS-1$
                    .toReturnStatement());
        } else {
            SimpleName results = f.newSimpleName("results"); //$NON-NLS-1$
            SimpleName attributes = f.newSimpleName("attributes"); //$NON-NLS-1$
            statements.add(newList(StageInput.class, results));
            statements.add(newMap(String.class, String.class, attributes));
            boolean dirty = false;
            for (MapReduceStageInfo.Input input : inputs) {
                if (dirty) {
                    statements.add(new ExpressionBuilder(f, attributes)
                        .method("clear") //$NON-NLS-1$
                        .toStatement());
                }
                for (Map.Entry<String, String> entry : input.attributes.entrySet()) {
                    statements.add(new ExpressionBuilder(f, attributes)
                        .method("put", literal(entry.getKey()), literal(entry.getValue())) //$NON-NLS-1$
                        .toStatement());
                    dirty = true;
                }
                statements.add(new ExpressionBuilder(f, results)
                    .method("add", new TypeBuilder(f, importer.toType(StageInput.class)) //$NON-NLS-1$
                        .newObject(
                                literal(input.path),
                                literal(input.formatClass),
                                literal(input.mapperClass),
                                attributes)
                        .toExpression())
                    .toStatement());
            }
            statements.add(f.newReturnStatement(results));
        }
        return Collections.singletonList(newGetter(METHOD_STAGE_INPUTS, listOf(StageInput.class), statements));
    }

    private List<MethodDeclaration> convertOutputs(List<MapReduceStageInfo.Output> outputs) {
        if (outputs.isEmpty()) {
            return Collections.emptyList();
        }
        SimpleName results = f.newSimpleName("results"); //$NON-NLS-1$
        SimpleName attributes = f.newSimpleName("attributes"); //$NON-NLS-1$
        List<Statement> statements = new ArrayList<>();
        statements.add(newList(StageOutput.class, results));
        statements.add(newMap(String.class, String.class, attributes));
        boolean dirty = false;
        for (MapReduceStageInfo.Output output : outputs) {
            if (dirty) {
                statements.add(new ExpressionBuilder(f, attributes)
                    .method("clear") //$NON-NLS-1$
                    .toStatement());
            }
            for (Map.Entry<String, String> entry : output.attributes.entrySet()) {
                statements.add(new ExpressionBuilder(f, attributes)
                    .method("put", literal(entry.getKey()), literal(entry.getValue())) //$NON-NLS-1$
                    .toStatement());
                dirty = true;
            }
            statements.add(new ExpressionBuilder(f, results)
                .method("add", new TypeBuilder(f, importer.toType(StageOutput.class)) //$NON-NLS-1$
                    .newObject(
                            literal(output.name),
                            literal(output.keyClass),
                            literal(output.valueClass),
                            literal(output.formatClass),
                            attributes)
                    .toExpression())
                .toStatement());
        }
        statements.add(f.newReturnStatement(results));
        return Collections.singletonList(newGetter(METHOD_STAGE_OUTPUTS, listOf(StageOutput.class), statements));
    }

    private List<MethodDeclaration> convertBaseOutput(String baseOutputPath) {
        return Collections.singletonList(newGetter(METHOD_STAGE_OUTPUT_PATH, String.class, literal(baseOutputPath)));
    }

    private List<MethodDeclaration> convertResources(List<MapReduceStageInfo.Resource> resources) {
        if (resources.isEmpty()) {
            return Collections.emptyList();
        }
        SimpleName results = f.newSimpleName("results"); //$NON-NLS-1$
        List<Statement> statements = new ArrayList<>();
        statements.add(newList(StageResource.class, results));
        for (MapReduceStageInfo.Resource resource : resources) {
            statements.add(new ExpressionBuilder(f, results)
                .method("add", new TypeBuilder(f, importer.toType(StageResource.class)) //$NON-NLS-1$
                    .newObject(literal(resource.path), literal(resource.name))
                    .toExpression())
                .toStatement());
        }
        statements.add(f.newReturnStatement(results));
        return Collections.singletonList(newGetter(METHOD_STAGE_RESOURCES, listOf(StageResource.class), statements));
    }

    private List<MethodDeclaration> convertShuffle(MapReduceStageInfo.Shuffle shuffle) {
        if (shuffle == null) {
            return Collections.emptyList();
        }
        List<MethodDeclaration> results = new ArrayList<>();
        addClassGetter(results, METHOD_SHUFFLE_KEY_CLASS, shuffle.keyClass);
        addClassGetter(results, METHOD_SHUFFLE_VALUE_CLASS, shuffle.valueClass);
        addClassGetter(results, METHOD_PARTITIONER_CLASS, shuffle.partitionerClass);
        addClassGetter(results, METHOD_COMBINER_CLASS, shuffle.combinerClass);
        addClassGetter(results, METHOD_SORT_COMPARATOR_CLASS, shuffle.sortComparatorClass);
        addClassGetter(results, METHOD_GROUPING_COMPARATOR_CLASS, shuffle.groupingComparatorClass);
        addClassGetter(results, METHOD_REDUCER_CLASS, shuffle.reducerClass);
        return results;
    }

    private void addClassGetter(List<? super MethodDeclaration> target, String name, ClassDescription value) {
        if (value == null) {
            return;
        }
        Type returnType = f.newParameterizedType(
                importer.toType(Class.class),
                importer.toType(JavaDomUtil.getName(value)));
        target.add(newGetter(name, returnType, literal(value)));
    }

    private Statement newMap(Class<?> key, Class<?> value, SimpleName name) {
        return new TypeBuilder(f, importer.toType(HashMap.class))
            .parameterize(importer.toType(key), importer.toType(value))
            .newObject()
            .toLocalVariableDeclaration(
                    new TypeBuilder(f, importer.toType(Map.class))
                        .parameterize(importer.toType(key), importer.toType(value))
                        .toType(),
                    name);
    }

    private Statement newList(Class<?> type, SimpleName name) {
        return new TypeBuilder(f, importer.toType(ArrayList.class))
            .parameterize(importer.toType(type))
            .newObject()
            .toLocalVariableDeclaration(listOf(type), name);
    }

    private Type listOf(Class<?> type) {
        return f.newParameterizedType(importer.toType(List.class), importer.toType(type));
    }

    private Literal literal(String value) {
        return Models.toLiteral(f, value);
    }

    private ClassLiteral literal(ClassDescription aClass) {
        Type type = importer.toType(JavaDomUtil.getName(aClass));
        return f.newClassLiteral(type);
    }

    private MethodDeclaration newGetter(String name, Class<?> type, Expression expression) {
        return newGetter(name, importer.toType(type), Collections.singletonList(f.newReturnStatement(expression)));
    }

    private MethodDeclaration newGetter(String name, Type type, Expression expression) {
        return newGetter(name, type, Collections.singletonList(f.newReturnStatement(expression)));
    }

    private MethodDeclaration newGetter(String name, Type type, List<? extends Statement> statements) {
        return f.newMethodDeclaration(
                null,
                new AttributeBuilder(f)
                    .annotation(importer.toType(Override.class))
                    .Public()
                    .toAttributes(),
                type,
                f.newSimpleName(name),
                Collections.<FormalParameterDeclaration>emptyList(),
                statements);
    }
}
