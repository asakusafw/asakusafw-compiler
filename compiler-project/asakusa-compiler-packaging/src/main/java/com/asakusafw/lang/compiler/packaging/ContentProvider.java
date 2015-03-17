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
package com.asakusafw.lang.compiler.packaging;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Provides binary contents.
 */
public interface ContentProvider {

    /**
     * Provides binary contents to the target stream.
     * The {@link OutputStream} will be closed after this method execution was finished,
     * so clients may or may not close it.
     * @param output the file contents output
     * @throws IOException if error occurred while executing this method
     */
    void writeTo(OutputStream output) throws IOException;
}