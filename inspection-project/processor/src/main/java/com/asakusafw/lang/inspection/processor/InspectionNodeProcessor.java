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
package com.asakusafw.lang.inspection.processor;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Collections;
import java.util.Map;

import com.asakusafw.lang.inspection.InspectionNode;

/**
 * Processes an {@link InspectionNode}.
 */
public interface InspectionNodeProcessor {

    /**
     * Processes the {@link InspectionNode} and output contents into the target stream.
     * @param context the current context
     * @param node the target inspection node
     * @param output the target output stream
     * @throws IOException if failed the target node
     */
    void process(Context context, InspectionNode node, OutputStream output) throws IOException;

    /**
     * Represents a context for {@link InspectionNodeProcessor}.
     */
    class Context {

        private final Map<String, String> options;

        /**
         * Creates a new instance w/ empty options.
         */
        public Context() {
            this(Collections.<String, String>emptyMap());
        }

        /**
         * Creates a new instance.
         * @param options the options
         */
        public Context(Map<String, String> options) {
            this.options = options;
        }

        /**
         * Returns the options.
         * @return the options
         */
        public Map<String, String> getOptions() {
            return options;
        }

        /**
         * Returns an option value.
         * @param key the option key
         * @param defaultValue the default value
         * @return the option value, or the default value if the target option is not defined
         */
        public String getOption(String key, String defaultValue) {
            String value = options.get(key);
            if (value == null) {
                return defaultValue;
            }
            return value;
        }

        /**
         * Returns an option value.
         * @param key the option key
         * @param defaultValue the default value
         * @return the option value, or the default value if the target option is not defined
         */
        public boolean getOption(String key, boolean defaultValue) {
            String value = options.get(key);
            if (value == null) {
                return defaultValue;
            }
            return Boolean.parseBoolean(value);
        }
    }
}
