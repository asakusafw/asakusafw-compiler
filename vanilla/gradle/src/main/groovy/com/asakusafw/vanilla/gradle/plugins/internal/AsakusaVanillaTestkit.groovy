/*
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
package com.asakusafw.vanilla.gradle.plugins.internal

import org.gradle.api.Project

import com.asakusafw.gradle.plugins.AsakusaTestkit
import com.asakusafw.gradle.plugins.AsakusafwPluginConvention
import com.asakusafw.gradle.plugins.internal.AsakusaSdkPlugin

/**
 * An implementation of {@link AsakusaTestkit} which uses Asakusa Vanilla.
 * @since 0.4.0
 */
class AsakusaVanillaTestkit implements AsakusaTestkit {

    @Override
    String getName() {
        return 'vanilla'
    }

    @Override
    int getPriority() {
        return 10
    }

    @Override
    void apply(Project project) {
        project.logger.info "enabling Vanilla Testkit (${name})"
        project.configurations {
            testCompile.extendsFrom asakusaVanillaTestkit
        }
    }

    @Override
    String toString() {
        return "Testkit(${name})"
    }
}
