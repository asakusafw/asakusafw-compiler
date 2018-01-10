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
package com.asakusafw.dag.api.processor.basic;

import com.asakusafw.dag.api.processor.TaskInfo;

/**
 * A basic implementation of {@link TaskInfo}.
 * @since 0.4.0
 */
public class BasicTaskInfo implements TaskInfo {

    private final Object value;

    /**
     * Creates a new instance.
     */
    public BasicTaskInfo() {
        this(null);
    }

    /**
     * Creates a new instance.
     * @param value the task value
     */
    public BasicTaskInfo(Object value) {
        this.value = value;
    }

    /**
     * Returns the value.
     * @return the value
     */
    public Object getValue() {
        return value;
    }
}
