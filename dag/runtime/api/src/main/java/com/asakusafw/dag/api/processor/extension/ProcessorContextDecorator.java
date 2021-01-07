/**
 * Copyright 2011-2021 Asakusa Framework Team.
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
package com.asakusafw.dag.api.processor.extension;

import com.asakusafw.dag.api.processor.ProcessorContext;
import com.asakusafw.dag.api.processor.TaskProcessorContext;
import com.asakusafw.dag.api.processor.VertexProcessorContext;

/**
 * Decorates {@link ProcessorContext}.
 * @since 0.4.0
 */
public interface ProcessorContextDecorator {

    /**
     * No extensions.
     */
    ProcessorContextDecorator NULL = new ProcessorContextDecorator() {
        // use default methods
    };

    /**
     * Enhances the {@link VertexProcessorContext} using this extension.
     * @param context the target context
     * @return the enhanced context
     */
    default VertexProcessorContext bless(VertexProcessorContext context) {
        return context;
    }

    /**
     * Enhances the {@link TaskProcessorContext} using this extension.
     * @param context the target context
     * @return the enhanced context
     */
    default TaskProcessorContext bless(TaskProcessorContext context) {
        return context;
    }
}
