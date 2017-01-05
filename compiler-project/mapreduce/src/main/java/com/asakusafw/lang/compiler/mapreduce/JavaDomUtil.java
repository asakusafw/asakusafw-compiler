/**
 * Copyright 2011-2017 Asakusa Framework Team.
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
import java.io.PrintWriter;

import com.asakusafw.lang.compiler.javac.JavaSourceExtension;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.utils.java.model.syntax.CompilationUnit;
import com.asakusafw.utils.java.model.syntax.ModelFactory;
import com.asakusafw.utils.java.model.syntax.Name;
import com.asakusafw.utils.java.model.syntax.PackageDeclaration;
import com.asakusafw.utils.java.model.syntax.TypeDeclaration;
import com.asakusafw.utils.java.model.util.Emitter;
import com.asakusafw.utils.java.model.util.ImportBuilder;
import com.asakusafw.utils.java.model.util.Models;

/**
 * Utilities for Java DOM.
 */
public final class JavaDomUtil {

    private static final ModelFactory F = Models.getModelFactory();

    private JavaDomUtil() {
        return;
    }

    /**
     * Returns a name of the target class.
     * @param aClass the target class
     * @return the related name
     */
    public static Name getName(ClassDescription aClass) {
        return Models.toName(F, aClass.getClassName());
    }

    /**
     * Creates a new {@link ImportBuilder} for generating the target class.
     * @param aClass the target class
     * @return the created instance
     */
    public static ImportBuilder newImportBuilder(ClassDescription aClass) {
        ImportBuilder result = new ImportBuilder(F, getPackageDeclaration(aClass), ImportBuilder.Strategy.TOP_LEVEL);
        return result;
    }

    private static PackageDeclaration getPackageDeclaration(ClassDescription aClass) {
        String packageName = aClass.getPackageName();
        if (packageName == null) {
            return null;
        } else {
            return F.newPackageDeclaration(Models.toName(F, packageName));
        }
    }

    /**
     * Emits a compilation unit.
     * @param unit the target compilation unit
     * @param javac the target java compiler
     * @return the primary class name
     * @throws IOException if failed to emit the compilation unit by I/O error
     */
    public static ClassDescription emit(CompilationUnit unit, JavaSourceExtension javac) throws IOException {
        ClassDescription aClass = findPrimaryClass(unit);
        if (aClass == null) {
            throw new IllegalArgumentException();
        }
        try (PrintWriter writer = new PrintWriter(javac.addJavaFile(aClass))) {
            Models.emit(unit, writer);
        }
        return aClass;
    }

    private static ClassDescription findPrimaryClass(CompilationUnit unit) {
        StringBuilder buf = new StringBuilder();
        if (unit.getPackageDeclaration() != null) {
            buf.append(unit.getPackageDeclaration().getName().toNameString());
            buf.append('.');
        }
        TypeDeclaration type = Emitter.findPrimaryType(unit);
        if (type == null) {
            return null;
        }
        buf.append(type.getName().toNameString());
        return new ClassDescription(buf.toString());
    }
}
