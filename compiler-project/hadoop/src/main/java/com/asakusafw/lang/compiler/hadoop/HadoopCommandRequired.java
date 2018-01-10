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
package com.asakusafw.lang.compiler.hadoop;

import java.text.MessageFormat;

import com.asakusafw.lang.compiler.api.reference.TaskReference;

/**
 * Represents an attribute whether the target {@link TaskReference} requires Hadoop command or not.
 * @since 0.3.0
 */
public class HadoopCommandRequired {

    /**
     * The default value of this attribute.
     */
    public static final boolean DEFAULT_VALUE = true;

    private final boolean required;

    /**
     * Creates a new instance.
     * @param value {@code true} if the target task requires Hadoop command, otherwise {@code false}
     */
    public HadoopCommandRequired(boolean value) {
        this.required = value;
    }

    /**
     * Returns whether the target task requires Hadoop command or not.
     * @param reference the target task
     * @return {@code true} if the target task requires Hadoop command, otherwise {@code false}
     * @see #DEFAULT_VALUE
     */
    public static boolean get(TaskReference reference) {
        HadoopCommandRequired attribute = reference.getAttribute(HadoopCommandRequired.class);
        if (attribute == null) {
            return DEFAULT_VALUE;
        }
        return attribute.required;
    }

    /**
     * Puts whether the target task requires Hadoop command or not.
     * @param <T> the target task reference type
     * @param reference the target task reference
     * @param value {@code true} if the target task requires Hadoop command, otherwise {@code false}
     * @return the target task reference
     */
    public static <T extends TaskReference> T put(T reference, boolean value) {
        reference.putAttribute(HadoopCommandRequired.class, new HadoopCommandRequired(value));
        return reference;
    }

    /**
     * Returns whether the target task requires Hadoop command or not.
     * @return {@code true} if the target task requires Hadoop command, otherwise {@code false}
     */
    public boolean isRequired() {
        return required;
    }

    @Override
    public String toString() {
        return MessageFormat.format(
                "HadoopCommandRequired({0})", //$NON-NLS-1$
                isRequired());
    }
}
