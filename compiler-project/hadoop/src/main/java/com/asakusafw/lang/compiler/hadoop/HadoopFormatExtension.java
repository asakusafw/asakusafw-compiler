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
package com.asakusafw.lang.compiler.hadoop;

import com.asakusafw.lang.compiler.common.ExtensionContainer;
import com.asakusafw.lang.compiler.model.description.ClassDescription;

/**
 * An extension for using Hadoop input/output formats.
 */
public class HadoopFormatExtension {

    /**
     * Input format class for Asakusa temporary dataset.
     */
    public static final ClassDescription DEFAULT_INPUT_FORMAT =
            new ClassDescription("com.asakusafw.runtime.stage.input.TemporaryInputFormat"); //$NON-NLS-1$

    /**
     * Output format class for Asakusa temporary dataset.
     */
    public static final ClassDescription DEFAULT_OUTPUT_FORMAT =
            new ClassDescription("com.asakusafw.runtime.stage.output.TemporaryOutputFormat"); //$NON-NLS-1$

    /**
     * The default configuration.
     */
    public static final HadoopFormatExtension DEFAULT =
            new HadoopFormatExtension(DEFAULT_INPUT_FORMAT, DEFAULT_OUTPUT_FORMAT);

    private final ClassDescription inputFormat;

    private final ClassDescription outputFormat;

    /**
     * Creates a new instance.
     * @param inputFormat the input format type
     * @param outputFormat the output format type
     */
    public HadoopFormatExtension(ClassDescription inputFormat, ClassDescription outputFormat) {
        this.inputFormat = inputFormat;
        this.outputFormat = outputFormat;
    }

    /**
     * Returns the input format type.
     * @return the input format type
     */
    public ClassDescription getInputFormat() {
        return inputFormat;
    }

    /**
     * Returns the output format type.
     * @return the output format type
     */
    public ClassDescription getOutputFormat() {
        return outputFormat;
    }

    /**
     * Returns the input format type for the target context.
     * @param context the target context
     * @return the input format type
     */
    public static ClassDescription getInputFormat(ExtensionContainer context) {
        return get(context).getInputFormat();
    }

    /**
     * Returns the output format type for the target context.
     * @param context the target context
     * @return the output format type
     */
    public static ClassDescription getOutputFormat(ExtensionContainer context) {
        return get(context).getOutputFormat();
    }

    private static HadoopFormatExtension get(ExtensionContainer context) {
        HadoopFormatExtension extension = context.getExtension(HadoopFormatExtension.class);
        if (extension == null) {
            return DEFAULT;
        }
        return extension;
    }
}
