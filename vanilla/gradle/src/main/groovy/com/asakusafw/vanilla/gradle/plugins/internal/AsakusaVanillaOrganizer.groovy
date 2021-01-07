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
package com.asakusafw.vanilla.gradle.plugins.internal

import org.gradle.api.Project
import org.gradle.api.Task
import org.gradle.api.file.FileCopyDetails

import com.asakusafw.gradle.plugins.AsakusafwBaseExtension
import com.asakusafw.gradle.plugins.AsakusafwBasePlugin
import com.asakusafw.gradle.plugins.AsakusafwOrganizerProfile
import com.asakusafw.gradle.plugins.internal.AbstractOrganizer
import com.asakusafw.gradle.plugins.internal.PluginUtils
import com.asakusafw.vanilla.gradle.plugins.AsakusafwOrganizerVanillaExtension

/**
 * Processes an {@link AsakusafwOrganizerProfile} for Asakusa Vanilla.
 * @since 0.4.0
 */
class AsakusaVanillaOrganizer extends AbstractOrganizer {

    private final AsakusafwOrganizerVanillaExtension extension

    /**
     * Creates a new instance.
     * @param project the current project
     * @param profile the target profile
     */
    AsakusaVanillaOrganizer(Project project,
            AsakusafwOrganizerProfile profile,
            AsakusafwOrganizerVanillaExtension extension) {
        super(project, profile)
        this.extension = extension
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
            VanillaDist : "Contents of Asakusa Vanilla modules (${profile.name}).",
            VanillaLib : "Libraries of Asakusa Vanilla modules (${profile.name}).",
        ])
    }

    private void configureDependencies() {
        PluginUtils.afterEvaluate(project) {
            AsakusafwBaseExtension base = AsakusafwBasePlugin.get(project)
            AsakusaVanillaBaseExtension vanilla = AsakusaVanillaBasePlugin.get(project)
            createDependencies('asakusafw', [
                VanillaDist : [
                    "com.asakusafw.vanilla.runtime:asakusa-vanilla-assembly:${vanilla.featureVersion}:dist@jar",
                    "com.asakusafw.vanilla.runtime:asakusa-vanilla-bootstrap:${vanilla.featureVersion}:dist@jar",
                ],
                VanillaLib : [
                    "com.asakusafw.vanilla.runtime:asakusa-vanilla-assembly:${vanilla.featureVersion}:lib@jar",
                    "com.asakusafw.vanilla.runtime:asakusa-vanilla-bootstrap:${vanilla.featureVersion}:exec@jar",
                    "ch.qos.logback:logback-classic:${base.logbackVersion}",
                ],
            ])
        }
    }

    private void createDependency(String configurationName, Object notation, Closure<?> configurator) {
        project.dependencies.add(qualify(configurationName), notation, configurator)
    }

    private void configureTasks() {
        createAttachComponentTasks 'attachComponent', [
            Vanilla : {
                into('.') {
                    extract configuration('asakusafwVanillaDist')
                    process {
                        filesMatching('**/vanilla/bin/execute') { FileCopyDetails f ->
                            f.setMode(0755)
                        }
                    }
                }
                into('vanilla/lib') {
                    put configuration('asakusafwVanillaLib')
                    process {
                        rename(/(asakusa-vanilla-bootstrap)-.*-exec\.jar/, '$1.jar')
                    }
                }
            },
        ]
        createAttachComponentTasks 'attach', [
            VanillaBatchapps : {
                into('batchapps') {
                    put project.asakusafw.vanilla.outputDirectory
                }
            },
        ]
    }

    private void enableTasks() {
        PluginUtils.afterEvaluate(project) {
            if (extension.isEnabled()) {
                project.logger.info "Enabling Asakusa Vanilla (${profile.name})"
                task('attachAssemble').dependsOn task('attachComponentVanilla')
                PluginUtils.afterTaskEnabled(project, AsakusaVanillaSdkPlugin.TASK_COMPILE) { Task compiler ->
                    task('attachVanillaBatchapps').dependsOn compiler
                    if (profile.batchapps.isEnabled()) {
                        project.logger.info "Enabling Vanilla Batchapps (${profile.name})"
                        task('attachAssemble').dependsOn task('attachVanillaBatchapps')
                    }
                }
            }
        }
    }
}
