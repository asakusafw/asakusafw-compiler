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

import org.gradle.api.NamedDomainObjectCollection
import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.Task

import com.asakusafw.gradle.plugins.AsakusafwOrganizerPlugin
import com.asakusafw.gradle.plugins.AsakusafwOrganizerPluginConvention
import com.asakusafw.gradle.plugins.AsakusafwOrganizerProfile

/**
 * A Gradle sub plug-in for Asakusa Language projects organizer.
 * @since 0.4.2
 */
class AsakusaLangOrganizerPlugin implements Plugin<Project> {

    private Project project

    private NamedDomainObjectCollection<AsakusaLangOrganizer> organizers

    @Override
    void apply(Project project) {
        this.project = project
        this.organizers = project.container(AsakusaLangOrganizer)

        project.apply plugin: 'asakusafw-organizer'
        project.apply plugin: AsakusaLangBasePlugin

        configureProfiles()
        configureTasks()
    }

    /**
     * Returns the organizers for each profile (only for testing).
     * @return the organizers for each profile
     */
    NamedDomainObjectCollection<AsakusaLangOrganizer> getOrganizers() {
        return organizers
    }

    private void configureProfiles() {
        AsakusafwOrganizerPluginConvention convention = project.asakusafwOrganizer
        convention.profiles.all { AsakusafwOrganizerProfile profile ->
            configureProfile(profile)
        }
    }

    private void configureProfile(AsakusafwOrganizerProfile profile) {
        AsakusaLangOrganizer organizer = new AsakusaLangOrganizer(project, profile)
        organizer.configureProfile()
        organizers << organizer
    }

    private void configureTasks() {
        // no tasks
    }

    private void defineFacadeTasks(Map<String, String> taskMap) {
        taskMap.each { String taskName, String desc ->
            project.task(taskName) { Task task ->
                if (desc != null) {
                    task.group AsakusafwOrganizerPlugin.ASAKUSAFW_ORGANIZER_GROUP
                    task.description desc
                }
                organizers.all { AsakusaLangOrganizer organizer ->
                    task.dependsOn organizer.task(task.name)
                }
            }
        }
    }
}
