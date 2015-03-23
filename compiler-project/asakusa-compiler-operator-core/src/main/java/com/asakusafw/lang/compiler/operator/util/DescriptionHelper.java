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
import java.util.List;

import com.asakusafw.lang.compiler.model.description.ArrayDescription;
import com.asakusafw.lang.compiler.model.description.ArrayTypeDescription;
import com.asakusafw.lang.compiler.model.description.BasicTypeDescription;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.description.EnumConstantDescription;
import com.asakusafw.lang.compiler.model.description.ImmediateDescription;
import com.asakusafw.lang.compiler.model.description.ReifiableTypeDescription;
import com.asakusafw.lang.compiler.model.description.TypeDescription;
import com.asakusafw.lang.compiler.model.description.ValueDescription;
import com.asakusafw.utils.java.model.syntax.ArrayType;
import com.asakusafw.utils.java.model.syntax.BasicTypeKind;
import com.asakusafw.utils.java.model.syntax.Expression;
import com.asakusafw.utils.java.model.syntax.ModelFactory;
import com.asakusafw.utils.java.model.syntax.Type;
import com.asakusafw.utils.java.model.util.ImportBuilder;
import com.asakusafw.utils.java.model.util.Models;
import com.asakusafw.utils.java.model.util.TypeBuilder;

/**
 * Common helper methods about descriptions.
 */
public final class DescriptionHelper {

    private DescriptionHelper() {
        return;
    }

    /**
     * Returns the resolved type.
     * @param importer the current import builder
     * @param type the target class description
     * @return the resolved type
     */
    public static Type resolve(ImportBuilder importer, TypeDescription type) {
        switch (type.getTypeKind()) {
        case BASIC:
            return resolve((BasicTypeDescription) type);
        case CLASS:
            return resolve(importer, (ClassDescription) type);
        case ARRAY:
            return resolve(importer, (ArrayTypeDescription) type);
        default:
            throw new AssertionError(type);
        }
    }

    private static Type resolve(BasicTypeDescription type) {
        ModelFactory factory = Models.getModelFactory();
        switch (type.getBasicTypeKind()) {
        case BOOLEAN:
            return factory.newBasicType(BasicTypeKind.BOOLEAN);
        case BYTE:
            return factory.newBasicType(BasicTypeKind.BYTE);
        case CHAR:
            return factory.newBasicType(BasicTypeKind.CHAR);
        case DOUBLE:
            return factory.newBasicType(BasicTypeKind.DOUBLE);
        case FLOAT:
            return factory.newBasicType(BasicTypeKind.FLOAT);
        case INT:
            return factory.newBasicType(BasicTypeKind.INT);
        case LONG:
            return factory.newBasicType(BasicTypeKind.LONG);
        case SHORT:
            return factory.newBasicType(BasicTypeKind.SHORT);
        case VOID:
            return factory.newBasicType(BasicTypeKind.VOID);
        default:
            throw new AssertionError(type);
        }
    }

    private static Type resolve(ImportBuilder importer, ClassDescription type) {
        ModelFactory factory = Models.getModelFactory();
        return importer.toType(Models.toName(factory, type.getClassName()));
    }

    private static Type resolve(ImportBuilder importer, ArrayTypeDescription type) {
        int dim = 1;
        TypeDescription current = type.getComponentType();
        while (current.getTypeKind() == TypeDescription.TypeKind.ARRAY) {
            current = ((ArrayTypeDescription) current).getComponentType();
            dim++;
        }
        ModelFactory factory = Models.getModelFactory();
        Type result = resolve(importer, current);
        for (int i = 0; i < dim; i++) {
            result = factory.newArrayType(result);
        }
        return result;
    }

    /**
     * Returns the resolved constant value.
     * @param importer the current import builder
     * @param constant the target constant value
     * @return the resolved expression
     */
    public static Expression resolveConstant(ImportBuilder importer, ValueDescription constant) {
        switch (constant.getValueKind()) {
        case IMMEDIATE:
            return resolveConstant((ImmediateDescription) constant);
        case ENUM_CONSTANT:
            return resolveConstant(importer, (EnumConstantDescription) constant);
        case TYPE:
            return resolveConstant(importer, (ReifiableTypeDescription) constant);
        case ARRAY:
            return resolveConstant(importer, (ArrayDescription) constant);
        default:
            throw new IllegalArgumentException(MessageFormat.format(
                    "not a constant value: {0}",
                    constant));
        }
    }

    private static Expression resolveConstant(ImmediateDescription constant) {
        ModelFactory factory = Models.getModelFactory();
        Object value = constant.getValue();
        if (value == null) {
            return Models.toNullLiteral(factory);
        } else {
            return Models.toLiteral(factory, value);
        }
    }

    private static Expression resolveConstant(ImportBuilder importer, EnumConstantDescription constant) {
        ModelFactory factory = Models.getModelFactory();
        Type type = resolve(importer, constant.getDeclaringClass());
        return new TypeBuilder(factory, type).field(constant.getName()).toExpression();
    }

    private static Expression resolveConstant(ImportBuilder importer, ReifiableTypeDescription constant) {
        ModelFactory factory = Models.getModelFactory();
        return factory.newClassLiteral(resolve(importer, constant));
    }

    private static Expression resolveConstant(ImportBuilder importer, ArrayDescription constant) {
        ModelFactory factory = Models.getModelFactory();
        ArrayType type = (ArrayType) resolve(importer, constant.getValueType());
        List<Expression> elements = new ArrayList<>();
        for (ValueDescription value : constant.getElements()) {
            elements.add(resolveConstant(importer, value));
        }
        return factory.newArrayCreationExpression(type, factory.newArrayInitializer(elements));
    }
}
