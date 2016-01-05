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
package com.asakusafw.lang.compiler.extension.directio;

import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.description.Descriptions;
import com.asakusafw.runtime.stage.input.BridgeInputFormat;
import com.asakusafw.runtime.stage.output.BridgeOutputFormat;

/**
 * Constants for Direct file I/Os.
 */
public final class DirectFileIoConstants {

    /**
     * Hadoop input format class for direct file inputs.
     */
    public static final ClassDescription CLASS_INPUT_FORMAT = Descriptions.classOf(BridgeInputFormat.class);

    /**
     * Hadoop output format class for direct file outputs.
     */
    public static final ClassDescription CLASS_OUTPUT_FORMAT = Descriptions.classOf(BridgeOutputFormat.class);

    private DirectFileIoConstants() {
        return;
    }
}
