/*
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
package com.asakusafw.lang.gradle.plugins.internal

import org.gradle.api.Project

import com.asakusafw.gradle.plugins.AsakusafwBaseExtension
import com.asakusafw.gradle.plugins.AsakusafwBasePlugin
import com.asakusafw.gradle.plugins.AsakusafwOrganizerProfile
import com.asakusafw.gradle.plugins.internal.AbstractOrganizer
import com.asakusafw.gradle.plugins.internal.PluginUtils

/**
 * Processes an {@link AsakusafwOrganizerProfile} for Asakusa Language projects.
 * @since 0.4.2
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
        configureConfigurations()
        configureDependencies()
        configureTasks()
        enableTasks()
    }

    private void configureConfigurations() {
        createConfigurations('asakusafw', [
            LangToolsDist : "Contents of Asakusa Language tools modules (${profile.name}).",
            LangToolsLib : "Libraries of Asakusa Language tools modules (${profile.name}).",
        ])
    }

    private void configureDependencies() {
        PluginUtils.afterEvaluate(project) {
            AsakusafwBaseExtension base = AsakusafwBasePlugin.get(project)
            AsakusaLangBaseExtension lang = AsakusaLangBasePlugin.get(project)
            createDependencies('asakusafw', [
                LangToolsDist : [
                    "com.asakusafw.lang.info:asakusa-info-cli:${lang.featureVersion}:dist@jar"
                ],
                LangToolsLib : [
                    "com.asakusafw.lang.info:asakusa-info-cli:${lang.featureVersion}:exec@jar"
                ],
            ])
        }
    }

    private void createDependency(String configurationName, Object notation, Closure<?> configurator) {
        project.dependencies.add(qualify(configurationName), notation, configurator)
    }

    private void configureTasks() {
        createAttachComponentTasks 'attachComponent', [
            LangTools : {
                into('.') {
                    extract configuration('asakusafwLangToolsDist')
                }
                into('tools/lib') {
                    put configuration('asakusafwLangToolsLib')
                    process {
                        rename(/([0-9A-Za-z\-]+)-cli-.*-exec.jar/, '$1.jar')
                    }
                }
            },
        ]
    }

    private void enableTasks() {
        PluginUtils.afterEvaluate(project) {
            task('attachAssemble').dependsOn task('attachComponentLangTools')
        }
    }
}
