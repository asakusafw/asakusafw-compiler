/**
 * Copyright 2011-2018 Asakusa Framework Team.
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
package com.asakusafw.dag.compiler.model;

import java.io.IOException;
import java.io.OutputStream;
import java.text.MessageFormat;
import java.util.function.Supplier;

import com.asakusafw.lang.compiler.common.Diagnostic.Level;
import com.asakusafw.lang.compiler.common.DiagnosticException;
import com.asakusafw.lang.compiler.common.Location;
import com.asakusafw.lang.compiler.common.ResourceContainer;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.utils.common.Arguments;
import com.asakusafw.lang.utils.common.Invariants;

/**
 * Represents a Java class data.
 * @since 0.4.0
 */
public class ClassData {

    private static final byte[] EMPTY = new byte[0];

    private final ClassDescription aClass;

    private final byte[] classFile;

    /**
     * Creates a new instance for provided classes.
     * @param aClass the target class
     */
    public ClassData(ClassDescription aClass) {
        this(aClass, () -> EMPTY);
    }

    /**
     * Creates a new instance.
     * @param aClass the target class
     * @param classFile the class file contents
     */
    public ClassData(ClassDescription aClass, byte[] classFile) {
        this(aClass, Arguments.requireNonNull(classFile)::clone);
    }

    /**
     * Creates a new instance.
     * @param aClass the target class
     * @param classFile the class file contents supplier
     */
    public ClassData(ClassDescription aClass, Supplier<byte[]> classFile) {
        Arguments.requireNonNull(aClass);
        Arguments.requireNonNull(classFile);
        this.aClass = aClass;
        this.classFile = classFile.get();
    }

    /**
     * Returns the target class.
     * @return the target class
     */
    public ClassDescription getDescription() {
        return aClass;
    }

    /**
     * Returns whether this has actual class file contents or not.
     * If not, this only refers the other existing class.
     * @return {@code true} if the target has contents, otherwise {@code false}
     */
    public boolean hasContents() {
        return classFile.length != 0;
    }

    /**
     * Puts the class file contents onto the target resource container.
     * @param target the target resource container
     * @throws DiagnosticException if error was occurred while writing the class file
     */
    public void dump(ResourceContainer target) {
        if (hasContents()) {
            Location location = Location.of(getDescription().getInternalName() + ".class"); //$NON-NLS-1$
            try (OutputStream output = target.addResource(location)) {
                dump(output);
            } catch (IOException e) {
                throw new DiagnosticException(
                        Level.ERROR,
                        MessageFormat.format(
                                "error occurred while adding a class file: {0}",
                                getDescription().getBinaryName()),
                        e);
            }
        }
    }

    /**
     * Puts the class file contents into the output.
     * @param output the target output
     * @throws IOException if I/O exception was occurred while writing the contents
     */
    public void dump(OutputStream output) throws IOException {
        Arguments.requireNonNull(output);
        Invariants.require(hasContents());
        output.write(classFile);
    }

    @Override
    public String toString() {
        return String.format(
                "ClassData(%s, file=%,d)", //$NON-NLS-1$
                getDescription().getClassName(),
                classFile.length);
    }
}
