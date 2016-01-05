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
package com.asakusafw.lang.compiler.extension.directio.emitter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asakusafw.lang.compiler.api.reference.DataModelReference;
import com.asakusafw.lang.compiler.api.reference.PropertyReference;
import com.asakusafw.lang.compiler.extension.directio.OutputPattern.CompiledOrder;
import com.asakusafw.lang.compiler.javac.JavaSourceExtension;
import com.asakusafw.lang.compiler.mapreduce.JavaDomUtil;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.description.TypeDescription;
import com.asakusafw.runtime.io.util.InvertOrder;
import com.asakusafw.runtime.stage.directio.DirectOutputOrder;
import com.asakusafw.utils.java.model.syntax.Comment;
import com.asakusafw.utils.java.model.syntax.CompilationUnit;
import com.asakusafw.utils.java.model.syntax.ConstructorDeclaration;
import com.asakusafw.utils.java.model.syntax.Expression;
import com.asakusafw.utils.java.model.syntax.FieldDeclaration;
import com.asakusafw.utils.java.model.syntax.FormalParameterDeclaration;
import com.asakusafw.utils.java.model.syntax.MethodDeclaration;
import com.asakusafw.utils.java.model.syntax.ModelFactory;
import com.asakusafw.utils.java.model.syntax.SimpleName;
import com.asakusafw.utils.java.model.syntax.Statement;
import com.asakusafw.utils.java.model.syntax.Type;
import com.asakusafw.utils.java.model.syntax.TypeBodyDeclaration;
import com.asakusafw.utils.java.model.syntax.TypeDeclaration;
import com.asakusafw.utils.java.model.syntax.TypeParameterDeclaration;
import com.asakusafw.utils.java.model.util.AttributeBuilder;
import com.asakusafw.utils.java.model.util.ExpressionBuilder;
import com.asakusafw.utils.java.model.util.ImportBuilder;
import com.asakusafw.utils.java.model.util.JavadocBuilder;
import com.asakusafw.utils.java.model.util.Models;
import com.asakusafw.utils.java.model.util.TypeBuilder;

/**
 * Emits a subclass of {@link DirectOutputOrder}.
 */
public final class OrderingClassEmitter {

    static final Logger LOG = LoggerFactory.getLogger(OrderingClassEmitter.class);

    private final ModelFactory f = Models.getModelFactory();

    private final ClassDescription targetClass;

    private final DataModelReference dataType;

    private final List<CompiledOrder> orderingInfo;

    private final ImportBuilder importer;

    private OrderingClassEmitter(ClassDescription targetClass, OutputStageInfo.Operation operation) {
        assert targetClass != null;
        assert operation != null;
        this.dataType = operation.dataModel;
        this.orderingInfo = operation.resourcePattern.getOrders();
        this.targetClass = targetClass;
        this.importer = JavaDomUtil.newImportBuilder(targetClass);
    }

    /**
     * Emits a class.
     * @param targetClass the target class
     * @param output the output information
     * @param javac the Java compiler
     * @throws IOException if failed to emit the class
     */
    public static void emit(
            ClassDescription targetClass,
            OutputStageInfo.Operation output, JavaSourceExtension javac) throws IOException {
        OrderingClassEmitter self = new OrderingClassEmitter(targetClass, output);
        CompilationUnit source = self.generate();
        JavaDomUtil.emit(source, javac);
    }

    private CompilationUnit generate() {
        TypeDeclaration type = createType();
        return f.newCompilationUnit(
                importer.getPackageDeclaration(),
                importer.toImportDeclarations(),
                Collections.singletonList(type),
                Collections.<Comment>emptyList());
    }

    private TypeDeclaration createType() {
        SimpleName name = f.newSimpleName(targetClass.getSimpleName());
        importer.resolvePackageMember(name);
        List<TypeBodyDeclaration> members = new ArrayList<>();
        members.addAll(createFields());
        members.add(createConstructor());
        members.add(createSetMethod());
        return f.newClassDeclaration(
                null,
                new AttributeBuilder(f)
                    .Public()
                    .Final()
                    .toAttributes(),
                name,
                Collections.<TypeParameterDeclaration>emptyList(),
                t(DirectOutputOrder.class),
                Collections.<Type>emptyList(),
                members);
    }

    private List<FieldDeclaration> createFields() {
        List<FieldDeclaration> results = new ArrayList<>();
        for (CompiledOrder order : orderingInfo) {
            results.add(f.newFieldDeclaration(
                    null,
                    new AttributeBuilder(f)
                        .Private()
                        .Final()
                        .toAttributes(),
                    t(order.getTarget().getType()),
                    f.newSimpleName(order.getTarget().getName().toMemberName()),
                    null));
        }
        return results;
    }

    private ConstructorDeclaration createConstructor() {
        List<Expression> arguments = new ArrayList<>();
        for (CompiledOrder order : orderingInfo) {
            Expression arg = new TypeBuilder(f, t(order.getTarget().getType()))
                .newObject()
                .toExpression();
            if (order.isAscend() == false) {
                arg = new TypeBuilder(f, t(InvertOrder.class))
                    .newObject(arg)
                    .toExpression();
            }
            arguments.add(arg);
        }
        List<Statement> statements = new ArrayList<>();
        statements.add(f.newSuperConstructorInvocation(arguments));
        int position = 0;
        for (CompiledOrder order : orderingInfo) {
            Expression obj = new ExpressionBuilder(f, f.newThis())
                .method("get", Models.toLiteral(f, position)) //$NON-NLS-1$
                .toExpression();
            if (order.isAscend() == false) {
                Expression invert = f.newParenthesizedExpression(new ExpressionBuilder(f, obj)
                    .castTo(t(InvertOrder.class))
                    .toExpression());
                obj = new ExpressionBuilder(f, invert)
                    .method("getEntity") //$NON-NLS-1$
                    .toExpression();
            }
            statements.add(new ExpressionBuilder(f, f.newThis())
                .field(order.getTarget().getName().toMemberName())
                .assignFrom(new ExpressionBuilder(f, obj)
                    .castTo(t(order.getTarget().getType()))
                    .toExpression())
                .toStatement());
            position++;
        }
        return f.newConstructorDeclaration(
                new JavadocBuilder(f)
                    .text("Creates a new instance.") //$NON-NLS-1$
                    .toJavadoc(),
                new AttributeBuilder(f)
                    .Public()
                    .toAttributes(),
                f.newSimpleName(targetClass.getSimpleName()),
                Collections.<FormalParameterDeclaration>emptyList(),
                statements);
    }

    private MethodDeclaration createSetMethod() {
        SimpleName raw = getArgumentName("rawObject"); //$NON-NLS-1$
        SimpleName object = getArgumentName("object"); //$NON-NLS-1$
        List<Statement> statements = new ArrayList<>();
        statements.add(new ExpressionBuilder(f, raw)
            .castTo(t(dataType.getDeclaration()))
            .toLocalVariableDeclaration(t(dataType.getDeclaration()), object));
        for (CompiledOrder order : orderingInfo) {
            PropertyReference property = order.getTarget();
            statements.add(new ExpressionBuilder(f, f.newThis())
                .field(property.getName().toMemberName())
                .method("copyFrom", Util.newGetter(object, property)) //$NON-NLS-1$
                .toStatement());
        }
        AttributeBuilder attributes = new AttributeBuilder(f);
        if (orderingInfo.isEmpty() == false) {
            attributes = attributes.annotation(
                    importer.toType(SuppressWarnings.class),
                    Models.toLiteral(f, "deprecation")); //$NON-NLS-1$
        }
        attributes.annotation(t(Override.class)).Public();

        return f.newMethodDeclaration(
                null,
                attributes.toAttributes(),
                t(void.class),
                f.newSimpleName("set"), //$NON-NLS-1$
                Collections.singletonList(f.newFormalParameterDeclaration(t(Object.class), raw)),
                statements);
    }

    private SimpleName getArgumentName(String pref) {
        assert pref != null;
        StringBuilder nameBuffer = new StringBuilder(pref);
        while (true) {
            boolean conflict = false;
            for (CompiledOrder order : orderingInfo) {
                if (order.getTarget().getName().toMemberName().contentEquals(nameBuffer)) {
                    conflict = true;
                    continue;
                }
            }
            if (conflict == false) {
                return f.newSimpleName(nameBuffer.toString());
            }
            nameBuffer.append('_');
        }
    }

    private Type t(java.lang.reflect.Type type) {
        assert type != null;
        return importer.toType(type);
    }

    private Type t(TypeDescription type) {
        if (type instanceof ClassDescription) {
            return importer.toType(JavaDomUtil.getName((ClassDescription) type));
        }
        throw new IllegalArgumentException(type.toString());
    }
}
