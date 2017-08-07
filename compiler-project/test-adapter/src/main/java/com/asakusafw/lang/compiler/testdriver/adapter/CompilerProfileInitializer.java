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
package com.asakusafw.lang.compiler.testdriver.adapter;

import java.util.Collection;
import java.util.Collections;

import com.asakusafw.lang.compiler.common.Location;
import com.asakusafw.lang.compiler.tester.CompilerProfile;
import com.asakusafw.testdriver.compiler.CompilerConfiguration;

/**
 * Initializes {@link CompilerProfile} for Asakusa DSL TestKit.
 * @since 0.8.0
 * @since 0.10.0
 */
@FunctionalInterface
public interface CompilerProfileInitializer {

    /**
     * Returns the set of Asakusa launcher paths.
     * @return the launcher paths
     * @since 0.9.0
     */
    default Collection<Location> getLauncherPaths() {
        return Collections.emptySet();
    }

    /**
     * Returns a collector of task attributes.
     * @return task attribute collector, or {@code null} if it is not defined
     * @since 0.10.0
     */
    default TaskAttributeCollector getTaskAttributeCollector() {
        return (jobflow, task) -> Collections.emptySet();
    }

    /**
     * Initializes the compiler profile.
     * @param profile the target profile
     * @param configuration the original configuration
     */
    void initialize(CompilerProfile profile, CompilerConfiguration configuration);
}
