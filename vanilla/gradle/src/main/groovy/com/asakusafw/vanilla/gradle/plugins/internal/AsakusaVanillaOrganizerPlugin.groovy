/*
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
package com.asakusafw.vanilla.gradle.plugins.internal

import org.gradle.api.NamedDomainObjectCollection
import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.Task

import com.asakusafw.gradle.plugins.AsakusafwOrganizerPlugin
import com.asakusafw.gradle.plugins.AsakusafwOrganizerPluginConvention
import com.asakusafw.gradle.plugins.AsakusafwOrganizerProfile
import com.asakusafw.gradle.plugins.internal.PluginUtils
import com.asakusafw.lang.gradle.plugins.internal.AsakusaLangOrganizerPlugin
import com.asakusafw.vanilla.gradle.plugins.AsakusafwOrganizerVanillaExtension

/**
 * A Gradle sub plug-in for Asakusa Vanilla project organizer.
 * @since 0.4.0
 */
class AsakusaVanillaOrganizerPlugin implements Plugin<Project> {

    private Project project

    private NamedDomainObjectCollection<AsakusaVanillaOrganizer> organizers

    @Override
    void apply(Project project) {
        this.project = project
        this.organizers = project.container(AsakusaVanillaOrganizer)

        project.apply plugin: AsakusaLangOrganizerPlugin
        project.apply plugin: AsakusaVanillaBasePlugin

        configureConvention()
        configureProfiles()
        configureTasks()
    }

    /**
     * Returns the organizers for each profile (only for testing).
     * @return the organizers for each profile
     */
    NamedDomainObjectCollection<AsakusaVanillaOrganizer> getOrganizers() {
        return organizers
    }

    private void configureConvention() {
        AsakusaVanillaBaseExtension base = AsakusaVanillaBasePlugin.get(project)
        AsakusafwOrganizerPluginConvention convention = project.asakusafwOrganizer
        AsakusafwOrganizerVanillaExtension extension = convention.extensions.create('vanilla', AsakusafwOrganizerVanillaExtension)
        extension.conventionMapping.with {
            enabled = { true }
        }
        PluginUtils.injectVersionProperty(extension, { base.featureVersion })
    }

    private void configureProfiles() {
        AsakusafwOrganizerPluginConvention convention = project.asakusafwOrganizer
        convention.profiles.all { AsakusafwOrganizerProfile profile ->
            configureProfile(profile)
        }
    }

    private void configureProfile(AsakusafwOrganizerProfile profile) {
        AsakusaVanillaBaseExtension base = AsakusaVanillaBasePlugin.get(project)
        AsakusafwOrganizerVanillaExtension extension = profile.extensions.create('vanilla', AsakusafwOrganizerVanillaExtension)
        AsakusafwOrganizerVanillaExtension parent = project.asakusafwOrganizer.vanilla
        extension.conventionMapping.with {
            enabled = { parent.enabled }
        }
        PluginUtils.injectVersionProperty(extension, { base.featureVersion })
        AsakusaVanillaOrganizer organizer = new AsakusaVanillaOrganizer(project, profile, extension)
        organizer.configureProfile()
        organizers << organizer
    }

    private void configureTasks() {
        defineFacadeTasks([
            attachComponentVanilla : 'Attaches Asakusa Vanilla components to assemblies.',
            attachVanillaBatchapps : 'Attaches Asakusa Vanilla batch applications to assemblies.',
        ])
    }

    private void defineFacadeTasks(Map<String, String> taskMap) {
        taskMap.each { String taskName, String desc ->
            project.task(taskName) { Task task ->
                if (desc != null) {
                    task.group AsakusafwOrganizerPlugin.ASAKUSAFW_ORGANIZER_GROUP
                    task.description desc
                }
                organizers.all { AsakusaVanillaOrganizer organizer ->
                    task.dependsOn organizer.task(task.name)
                }
            }
        }
    }
}
