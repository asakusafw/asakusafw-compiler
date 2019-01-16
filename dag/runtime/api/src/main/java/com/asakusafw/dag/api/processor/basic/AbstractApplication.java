/**
 * Copyright 2011-2019 Asakusa Framework Team.
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
package com.asakusafw.dag.api.processor.basic;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.text.MessageFormat;
import java.util.function.Supplier;

import com.asakusafw.dag.api.model.GraphInfo;
import com.asakusafw.lang.utils.common.Arguments;

/**
 * An abstract super class of DAG based applications.
 * @since 0.4.0
 */
public class AbstractApplication implements Supplier<GraphInfo> {

    private final String path;

    /**
     * Creates a new instance.
     * @param path the path to the serialized {@link GraphInfo}
     * @see GraphInfo#save(java.io.OutputStream, GraphInfo)
     */
    public AbstractApplication(String path) {
        Arguments.requireNonNull(path);
        this.path = path;
    }

    @Override
    public GraphInfo get() {
        try (InputStream input = open()) {
            if (input == null) {
                throw new FileNotFoundException(MessageFormat.format(
                        "missing DAG info: {0} (searching context: {1})",
                        path,
                        getClass().getName()));
            }
            return GraphInfo.load(input);
        } catch (IOException e) {
            throw new IllegalStateException(MessageFormat.format(
                    "error occurred while restoring GraphInfo: {0}",
                    path), e);
        }
    }

    private InputStream open() {
        return getClass().getClassLoader().getResourceAsStream(path);
    }
}
