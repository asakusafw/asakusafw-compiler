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
package com.asakusafw.lang.compiler.hadoop;

import com.asakusafw.lang.compiler.api.ExternalPortProcessor;
import com.asakusafw.lang.compiler.model.info.ExternalInputInfo;

/**
 * An interface who can provides {@link InputFormatInfo}.
 * The supported {@link ExternalPortProcessor} must
 */
public interface InputFormatInfoSupport {

    /**
     * Resolves external input and returns information about suitable {@code InputFormat} to access the input.
     * @param context the current context
     * @param name the target input name
     * @param info the structural information of the target external input
     * @return the resolved information, or {@code null} if this does not support the target input
     */
    InputFormatInfo resolveInput(ExternalPortProcessor.Context context, String name, ExternalInputInfo info);
}
