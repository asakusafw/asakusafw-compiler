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
package com.asakusafw.lang.compiler.operator.util;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import javax.lang.model.element.Element;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.TypeParameterElement;
import javax.lang.model.type.TypeKind;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.type.TypeVariable;
import javax.lang.model.util.Types;
import javax.tools.Diagnostic;

import com.asakusafw.lang.compiler.model.description.EnumConstantDescription;
import com.asakusafw.lang.compiler.operator.CompileEnvironment;
import com.asakusafw.lang.compiler.operator.Constants;
import com.asakusafw.lang.compiler.operator.model.ExternMirror;
import com.asakusafw.lang.compiler.operator.model.KeyMirror;
import com.asakusafw.lang.compiler.operator.model.OperatorDescription;
import com.asakusafw.lang.compiler.operator.model.OperatorDescription.Node;
import com.asakusafw.lang.compiler.operator.model.OperatorDescription.ParameterReference;
import com.asakusafw.lang.compiler.operator.model.OperatorDescription.Reference;
import com.asakusafw.lang.compiler.operator.model.OperatorDescription.Reference.Kind;
import com.asakusafw.lang.compiler.operator.model.OperatorElement;
import com.asakusafw.utils.java.jsr269.bridge.Jsr269;
import com.asakusafw.utils.java.model.syntax.ClassLiteral;
import com.asakusafw.utils.java.model.syntax.Expression;
import com.asakusafw.utils.java.model.syntax.FormalParameterDeclaration;
import com.asakusafw.utils.java.model.syntax.ModelFactory;
import com.asakusafw.utils.java.model.syntax.SimpleName;
import com.asakusafw.utils.java.model.syntax.Statement;
import com.asakusafw.utils.java.model.syntax.Type;
import com.asakusafw.utils.java.model.syntax.TypeParameterDeclaration;
import com.asakusafw.utils.java.model.util.ExpressionBuilder;
import com.asakusafw.utils.java.model.util.ImportBuilder;
import com.asakusafw.utils.java.model.util.Models;
import com.asakusafw.utils.java.model.util.TypeBuilder;

/**
 * Common helper methods about elements.
 */
public final class ElementHelper {

    /**
     * Validates {@link OperatorDescription} and raises errors when it is not valid.
     * @param environment current environment
     * @param element target element
     * @param description target description
     * @return {@code true} if is valid, otherwise {@code false}
     * @throws IllegalArgumentException if some parameters were {@code null}
     */
    public static boolean validate(
            CompileEnvironment environment,
            ExecutableElement element,
            OperatorDescription description) {
        if (environment == null) {
            throw new IllegalArgumentException("environment must not be null"); //$NON-NLS-1$
        }
        if (element == null) {
            throw new IllegalArgumentException("element must not be null"); //$NON-NLS-1$
        }
        if (description == null) {
            throw new IllegalArgumentException("description must not be null"); //$NON-NLS-1$
        }
        boolean valid = true;
        valid &= validateParameterNames(environment, element, description);
        valid &= validateOutputNames(environment, element, description);
        valid &= validateOutputType(environment, element, description);
        return valid;
    }

    private static boolean validateParameterNames(
            CompileEnvironment environment,
            ExecutableElement element,
            OperatorDescription description) {
        assert environment != null;
        assert element != null;
        assert description != null;
        boolean valid = true;
        Map<String, Node> names = new HashMap<>();
        for (Node node : description.getParameters()) {
            if (names.containsKey(node.getName())) {
                Node other = names.get(node.getName());
                environment.getProcessingEnvironment().getMessager().printMessage(Diagnostic.Kind.ERROR,
                        MessageFormat.format(
                                "Input name \"{0}\" is conflict to {1}",
                                node.getName(),
                                target(element, other.getReference())),
                        target(element, node.getReference()));
                valid = false;
            } else {
                names.put(node.getName(), node);
            }
        }
        return valid;
    }

    private static boolean validateOutputNames(
            CompileEnvironment environment,
            ExecutableElement element,
            OperatorDescription description) {
        assert environment != null;
        assert element != null;
        assert description != null;
        boolean valid = true;
        Map<String, Node> names = new HashMap<>();
        for (Node node : description.getOutputs()) {
            if (names.containsKey(node.getName())) {
                Node other = names.get(node.getName());
                environment.getProcessingEnvironment().getMessager().printMessage(Diagnostic.Kind.ERROR,
                        MessageFormat.format(
                                "Output name \"{0}\" is conflict to {1}",
                                target(element, other.getReference())),
                        target(element, node.getReference()));
                valid = false;
            } else {
                names.put(node.getName(), node);
            }
        }
        return valid;
    }

    private static boolean validateOutputType(
            CompileEnvironment environment,
            ExecutableElement element,
            OperatorDescription description) {
        assert environment != null;
        assert element != null;
        assert description != null;
        boolean valid = true;
        List<TypeVariable> vars = new ArrayList<>();
        for (Node input : description.getInputs()) {
            if (input.getType().getKind() == TypeKind.TYPEVAR) {
                vars.add((TypeVariable) input.getType());
            }
        }
        Types types = environment.getProcessingEnvironment().getTypeUtils();
        for (Node output : description.getOutputs()) {
            if (output.getType().getKind() == TypeKind.TYPEVAR) {
                boolean found = false;
                for (TypeVariable var : vars) {
                    if (types.isSameType(var, output.getType())) {
                        found = true;
                        break;
                    }
                }
                if (found == false) {
                    environment.getProcessingEnvironment().getMessager().printMessage(Diagnostic.Kind.ERROR,
                            MessageFormat.format(
                                    "Failed to infer output type \"{0}\"",
                                    ((TypeVariable) output.getType()).asElement().getSimpleName()),
                            target(element, output.getReference()));
                    valid = false;
                }
            }
        }
        return valid;
    }

    private static Element target(ExecutableElement element, Reference reference) {
        assert element != null;
        assert reference != null;
        if (reference.getKind() == Reference.Kind.PARAMETER) {
            int index = ((ParameterReference) reference).getLocation();
            if (index < element.getParameters().size()) {
                return element.getParameters().get(index);
            }
        }
        return element;
    }

    /**
     * Creates type parameters about operator element.
     * @param environment current environment
     * @param typeParameters target type parameter elements
     * @param imports import builder
     * @return generated syntax model
     * @throws IllegalArgumentException if some parameters were {@code null}
     */
    public static List<TypeParameterDeclaration> toTypeParameters(
            CompileEnvironment environment,
            List<? extends TypeParameterElement> typeParameters,
            ImportBuilder imports) {
        if (environment == null) {
            throw new IllegalArgumentException("environment must not be null"); //$NON-NLS-1$
        }
        if (typeParameters == null) {
            throw new IllegalArgumentException("typeParameters must not be null"); //$NON-NLS-1$
        }
        if (imports == null) {
            throw new IllegalArgumentException("imports must not be null"); //$NON-NLS-1$
        }
        ModelFactory factory = Models.getModelFactory();
        Jsr269 converter = new Jsr269(factory);
        List<TypeParameterDeclaration> results = new ArrayList<>();
        for (TypeParameterElement typeParameter : typeParameters) {
            List<Type> bounds = new ArrayList<>();
            for (TypeMirror type : typeParameter.getBounds()) {
                bounds.add(imports.resolve(converter.convert(type)));
            }
            results.add(factory.newTypeParameterDeclaration(
                    factory.newSimpleName(typeParameter.getSimpleName().toString()),
                    bounds));
        }
        return results;
    }

    /**
     * Creates a parameterized about operator element.
     * @param environment current environment
     * @param typeParameters target type parameter elements
     * @param rawNodeType parameterization target
     * @param imports import builder
     * @return generated syntax model
     * @throws IllegalArgumentException if some parameters were {@code null}
     */
    public static Type toParameterizedType(
            CompileEnvironment environment,
            List<? extends TypeParameterElement> typeParameters,
            Type rawNodeType,
            ImportBuilder imports) {
        if (environment == null) {
            throw new IllegalArgumentException("environment must not be null"); //$NON-NLS-1$
        }
        if (rawNodeType == null) {
            throw new IllegalArgumentException("rawNodeType must not be null"); //$NON-NLS-1$
        }
        if (imports == null) {
            throw new IllegalArgumentException("imports must not be null"); //$NON-NLS-1$
        }
        if (typeParameters.isEmpty()) {
            return rawNodeType;
        }
        ModelFactory factory = Models.getModelFactory();
        List<Type> typeArgs = new ArrayList<>();
        for (TypeParameterElement typeParameter : typeParameters) {
            typeArgs.add(factory.newNamedType(factory.newSimpleName(typeParameter.getSimpleName().toString())));
        }
        return factory.newParameterizedType(rawNodeType, typeArgs);
    }

    /**
     * Creates type parameter declarations about operator element.
     * @param environment current environment
     * @param element target operator element
     * @param imports import builder
     * @return generated syntax model
     * @throws IllegalArgumentException if some parameters were {@code null}
     */
    public static List<FormalParameterDeclaration> toParameters(
            CompileEnvironment environment,
            OperatorElement element,
            ImportBuilder imports) {
        if (environment == null) {
            throw new IllegalArgumentException("environment must not be null"); //$NON-NLS-1$
        }
        if (element == null) {
            throw new IllegalArgumentException("element must not be null"); //$NON-NLS-1$
        }
        if (imports == null) {
            throw new IllegalArgumentException("imports must not be null"); //$NON-NLS-1$
        }
        if (element.getDescription() == null) {
            throw new IllegalArgumentException("element.description must not be null"); //$NON-NLS-1$
        }
        ModelFactory factory = Models.getModelFactory();
        Jsr269 converter = new Jsr269(factory);
        List<FormalParameterDeclaration> results = new ArrayList<>();
        for (Node param : element.getDescription().getParameters()) {
            Type type;
            switch (param.getKind()) {
            case INPUT:
                type = new TypeBuilder(factory, DescriptionHelper.resolve(imports, Constants.TYPE_SOURCE))
                    .parameterize(imports.resolve(converter.convert(param.getType())))
                    .toType();
                break;
            case DATA:
                type = imports.resolve(converter.convert(param.getType()));
                break;
            default:
                throw new AssertionError(param.getKind());
            }
            results.add(factory.newFormalParameterDeclaration(
                    type,
                    factory.newSimpleName(param.getName())));
        }
        return results;
    }

    /**
     * Creates arguments about operator element.
     * @param environment current environment
     * @param element target operator element
     * @param imports import builder
     * @return generated syntax model
     * @throws IllegalArgumentException if some parameters were {@code null}
     */
    public static List<Expression> toArguments(
            CompileEnvironment environment,
            OperatorElement element,
            ImportBuilder imports) {
        if (environment == null) {
            throw new IllegalArgumentException("environment must not be null"); //$NON-NLS-1$
        }
        if (element == null) {
            throw new IllegalArgumentException("element must not be null"); //$NON-NLS-1$
        }
        if (imports == null) {
            throw new IllegalArgumentException("imports must not be null"); //$NON-NLS-1$
        }
        if (element.getDescription() == null) {
            throw new IllegalArgumentException("element.description must not be null"); //$NON-NLS-1$
        }
        ModelFactory factory = Models.getModelFactory();
        List<Expression> results = new ArrayList<>();
        for (Node param : element.getDescription().getParameters()) {
            results.add(factory.newSimpleName(param.getName()));
        }
        return results;
    }

    /**
     * Creates operator node constructor statements.
     * @param environment current environment
     * @param element target operator element
     * @param builderExpression the {@code FlowElementBuilder} instance expression
     * @param imports import builder
     * @return generated syntax model
     */
    public static List<Statement> toNodeConstructorStatements(
            CompileEnvironment environment,
            OperatorElement element,
            Expression builderExpression,
            ImportBuilder imports) {
        ModelFactory f = Models.getModelFactory();
        List<Statement> statements = new ArrayList<>();
        for (Node node : element.getDescription().getAllNodes()) {
            String method;
            List<Expression> arguments = new ArrayList<>();
            switch (node.getKind()) {
            case INPUT:
                method = "defineInput"; //$NON-NLS-1$
                arguments.add(Models.toLiteral(f, node.getName()));
                arguments.add(f.newSimpleName(node.getName()));
                break;
            case OUTPUT:
                method = "defineOutput"; //$NON-NLS-1$
                arguments.add(Models.toLiteral(f, node.getName()));
                arguments.add(resolveOutputType(environment, imports, node, element));
                break;
            case DATA:
                method = "defineData"; //$NON-NLS-1$
                arguments.add(Models.toLiteral(f, node.getName()));
                arguments.add(toLiteral(environment, node.getType(), imports));
                arguments.add(f.newSimpleName(node.getName()));
                break;
            default:
                throw new AssertionError(node.getKind());
            }
            ExpressionBuilder builder = new ExpressionBuilder(f, builderExpression).method(method, arguments);
            if (node.getKey() != null) {
                builder = builder.method("withKey", //$NON-NLS-1$
                        ElementHelper.toKeyInfo(environment, node.getKey(), imports));
            }
            if (node.getExtern() != null) {
                builder = builder.method("withExtern", //$NON-NLS-1$
                        ElementHelper.toExternInfo(environment, node.getExtern(), imports));
            }
            if (node.getReference().getKind() == Kind.PARAMETER) {
                builder = builder.method("withParameterIndex", //$NON-NLS-1$
                        Models.toLiteral(f, ((ParameterReference) node.getReference()).getLocation()));
            }
            statements.add(builder.toStatement());
        }
        for (EnumConstantDescription attribute : element.getDescription().getAttributes()) {
            statements.add(new ExpressionBuilder(f, builderExpression)
                    .method("defineAttribute", DescriptionHelper.resolveConstant(imports, attribute)) //$NON-NLS-1$
                    .toStatement());
        }
        SimpleName editorVar = f.newSimpleName("$editor$");
        statements.add(new ExpressionBuilder(f, builderExpression)
            .method("resolve")
            .toLocalVariableDeclaration(
                    DescriptionHelper.resolve(imports, Constants.TYPE_ELEMENT_EDITOR),
                    editorVar));
        for (Node node : element.getDescription().getOutputs()) {
            Expression type = resolveOutputType(environment, imports, node, element);
            statements.add(new ExpressionBuilder(f, f.newThis())
                .field(f.newSimpleName(node.getName()))
                .assignFrom(new ExpressionBuilder(f, editorVar)
                    .method("createSource", Models.toLiteral(f, node.getName()), type)
                    .toExpression())
                .toStatement());
        }
        return statements;
    }

    private static Expression resolveOutputType(
            CompileEnvironment environment,
            ImportBuilder imports,
            Node node,
            OperatorElement element) {
        assert node != null;
        assert element != null;
        TypeMirror type = node.getType();
        if (type.getKind() != TypeKind.TYPEVAR) {
            return toLiteral(environment, type, imports);
        } else {
            Types types = environment.getProcessingEnvironment().getTypeUtils();
            for (Node input : element.getDescription().getInputs()) {
                if (types.isSameType(input.getType(), type)) {
                    return Models.getModelFactory().newSimpleName(input.getName());
                }
            }
            throw new IllegalStateException(node.getName());
        }
    }

    private static Expression toKeyInfo(
            CompileEnvironment environment,
            KeyMirror element,
            ImportBuilder imports) {
        ModelFactory factory = Models.getModelFactory();
        ExpressionBuilder builder =
                new TypeBuilder(factory, DescriptionHelper.resolve(imports, Constants.TYPE_KEY_INFO)).newObject();
        for (KeyMirror.Group group : element.getGroup()) {
            builder = builder.method("group", Models.toLiteral(factory, group.getProperty().getName()));
        }
        for (KeyMirror.Order order : element.getOrder()) {
            String direction = order.getDirection().name().toLowerCase(Locale.ENGLISH);
            builder = builder.method(direction, Models.toLiteral(factory, order.getProperty().getName()));
        }
        return builder.toExpression();
    }

    private static Expression toExternInfo(
            CompileEnvironment environment,
            ExternMirror element,
            ImportBuilder imports) {
        ModelFactory factory = Models.getModelFactory();
        return new TypeBuilder(factory, DescriptionHelper.resolve(imports, Constants.TYPE_EXTERN_INFO))
            .newObject(
                    Models.toLiteral(factory, element.getName()),
                    toLiteral(environment, element.getDescription(), imports))
            .toExpression();
    }

    private static ClassLiteral toLiteral(
            CompileEnvironment environment,
            TypeMirror element,
            ImportBuilder imports) {
        ModelFactory factory = Models.getModelFactory();
        Jsr269 converter = new Jsr269(factory);
        return factory.newClassLiteral(imports.resolve(converter.convert(environment.getErasure(element))));
    }

    private ElementHelper() {
        return;
    }
}
