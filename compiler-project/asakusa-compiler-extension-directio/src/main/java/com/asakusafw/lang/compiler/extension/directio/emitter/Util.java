package com.asakusafw.lang.compiler.extension.directio.emitter;

import com.asakusafw.lang.compiler.api.reference.PropertyReference;
import com.asakusafw.lang.compiler.model.description.MethodDescription;
import com.asakusafw.utils.java.model.syntax.Expression;
import com.asakusafw.utils.java.model.syntax.ModelFactory;
import com.asakusafw.utils.java.model.util.ExpressionBuilder;
import com.asakusafw.utils.java.model.util.Models;

final class Util {

    private static final ModelFactory F = Models.getModelFactory();

    private Util() {
        return;
    }

    public static Expression newGetter(Expression object, PropertyReference property) {
        MethodDescription declaration = property.getDeclaration();
        return new ExpressionBuilder(F, object)
            .method(declaration.getName())
            .toExpression();
    }
}
