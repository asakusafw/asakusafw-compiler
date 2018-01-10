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
package com.asakusafw.dag.api.processor;

import java.util.List;
import java.util.OptionalInt;

/**
 * Represents a custom task schedule.
 * @since 0.4.0
 * @version 0.4.2
 */
@FunctionalInterface
public interface TaskSchedule {

    /**
     * Returns the custom tasks schedule.
     * @return the custom tasks schedule
     */
    List<? extends TaskInfo> getTasks();

    /**
     * Returns the number of max concurrency.
     * @return the number of max concurrency, or empty if it is not limited
     * @since 0.4.2
     */
    default OptionalInt getMaxConcurrency() {
        return OptionalInt.empty();
    }
}
