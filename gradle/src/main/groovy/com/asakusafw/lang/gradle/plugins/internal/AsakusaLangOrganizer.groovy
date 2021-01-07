/*
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
package com.asakusafw.lang.gradle.plugins.internal

import org.gradle.api.Project

import com.asakusafw.gradle.plugins.AsakusafwOrganizerProfile
import com.asakusafw.gradle.plugins.internal.AbstractOrganizer

/**
 * Processes an {@link AsakusafwOrganizerProfile} for Asakusa Language projects.
 * @since 0.4.2
 * @since 0.5.0
 */
class AsakusaLangOrganizer extends AbstractOrganizer {

    /**
     * Creates a new instance.
     * @param project the current project
     * @param profile the target profile
     */
    AsakusaLangOrganizer(Project project, AsakusafwOrganizerProfile profile) {
        super(project, profile)
    }

    /**
     * Configures the target profile.
     */
    @Override
    void configureProfile() {
        return
    }
}
