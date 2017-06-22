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
package com.asakusafw.lang.info.api;

import java.io.IOException;
import java.io.InputStream;

import com.asakusafw.lang.compiler.api.CompilerOptions;
import com.asakusafw.lang.compiler.common.Location;
import com.asakusafw.lang.compiler.model.graph.Batch;
import com.asakusafw.lang.compiler.model.graph.Jobflow;
import com.asakusafw.lang.info.Attribute;

/**
 * Collects {@link Attribute} from batch.
 * @since 0.4.1
 */
public interface AttributeCollector {

    /**
     * Collects {@link Attribute} from the given batch.
     * @param context the current context
     * @param batch the target batch
     */
    default void process(Context context, Batch batch) {
        return;
    }

    /**
     * Collects {@link Attribute} from the given jobflow.
     * @param context the current context
     * @param jobflow the target jobflow
     */
    default void process(Context context, Jobflow jobflow) {
        return;
    }

    /**
     * Represents a context object for {@link AttributeCollector}.
     * @since 0.4.1
     * @version 0.4.2
     */
    public interface Context {

        /**
         * Returns the compiler options.
         * @return the compiler options
         */
        CompilerOptions getOptions();

        /**
         * Returns the class loader to obtain the target application classes.
         * @return the class loader
         */
        ClassLoader getClassLoader();

        /**
         * Adds an attribute about the processing element.
         * @param attribute the attribute to add
         */
        void putAttribute(Attribute attribute);

        /**
         * Returns a resource from the current output.
         * @param location the resource location in the target package
         * @return the jobflow resource, or {@code null} if it does not exist
         * @throws IOException if failed to open the file
         * @since 0.4.2
         */
        InputStream findResourceFile(Location location) throws IOException;
    }
}
