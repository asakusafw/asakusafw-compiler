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
package com.asakusafw.vanilla.gradle.plugins.internal

import org.gradle.api.Project
import org.gradle.api.Task
import org.gradle.api.artifacts.Configuration

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
            VanillaHadoopLib : "Hadoop Libraries of Asakusa Vanilla modules (${profile.name}).",
        ])
        configuration('asakusafwVanillaHadoopLib').with { Configuration conf ->
            conf.transitive = true
            // use snappy-java is provided in asakusa-runtime-all
            conf.exclude group: 'org.xerial.snappy', module: 'snappy-java'
            conf.exclude group: 'org.slf4j', module: 'slf4j-log4j12'
            conf.exclude group: 'ch.qos.logback', module: 'logback-classic'
        }
    }

    private void configureDependencies() {
        PluginUtils.afterEvaluate(project) {
            AsakusafwBaseExtension base = AsakusafwBasePlugin.get(project)
            AsakusaVanillaBaseExtension vanilla = AsakusaVanillaBasePlugin.get(project)
            createDependencies('asakusafw', [
                VanillaDist : [
                    "com.asakusafw.vanilla.runtime:asakusa-vanilla-assembly:${vanilla.featureVersion}:dist@jar"
                ],
                VanillaLib : [
                    "com.asakusafw.vanilla.runtime:asakusa-vanilla-assembly:${vanilla.featureVersion}:lib@jar",
                    "ch.qos.logback:logback-classic:${base.logbackVersion}",
                ],
                VanillaHadoopLib : [
                    "org.apache.hadoop:hadoop-common:${vanilla.hadoopVersion}",
                    "org.apache.hadoop:hadoop-mapreduce-client-core:${vanilla.hadoopVersion}",
                ],
            ])
            configuration('asakusafwVanillaHadoopLib').resolutionStrategy.dependencySubstitution {
                substitute module('log4j:log4j') with module("org.slf4j:log4j-over-slf4j:${base.slf4jVersion}")
                substitute module('commons-logging:commons-logging') with module("org.slf4j:jcl-over-slf4j:${base.slf4jVersion}")
            }
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
                }
                into('vanilla/lib') {
                    put configuration('asakusafwVanillaLib')
                }
            },
            VanillaHadoop : {
                into('vanilla/lib/hadoop') {
                    put configuration('asakusafwVanillaHadoopLib')
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
                if (!extension.isUseSystemHadoop()) {
                    project.logger.info "Enabling Asakusa Vanilla Hadoop bundle (${profile.name})"
                    task('attachAssemble').dependsOn task('attachComponentVanillaHadoop')
                }
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
