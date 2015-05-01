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
package com.asakusafw.lang.compiler.javac;

import java.io.IOException;
import java.io.Writer;

import com.asakusafw.lang.compiler.model.description.ClassDescription;

/**
 * An extension for using Java source files.
 */
public interface JavaSourceExtension {

    /**
     * Adds a new Java source file and returns its output stream.
     * @param aClass the target class
     * @return the output stream to set the target file contents
     * @throws IOException if failed to create a new file
     */
    Writer addJavaFile(ClassDescription aClass) throws IOException;
}