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

import org.gradle.api.Plugin
import org.gradle.api.Project

import com.asakusafw.gradle.plugins.AsakusafwCompilerExtension
import com.asakusafw.gradle.plugins.AsakusafwPluginConvention
import com.asakusafw.gradle.plugins.internal.AsakusaSdkPlugin
import com.asakusafw.gradle.plugins.internal.PluginUtils
import com.asakusafw.gradle.tasks.AsakusaCompileTask
import com.asakusafw.gradle.tasks.internal.ResolutionUtils

/**
 * A Gradle sub plug-in for Asakusa Vanilla SDK.
 * @since 0.4.0
 * @see AsakusaVanillaSdkBasePlugin
 */
class AsakusaVanillaSdkPlugin implements Plugin<Project> {

    /**
     * The compile task name.
     */
    public static final String TASK_COMPILE = 'vanillaCompileBatchapps'

    private Project project

    private AsakusafwCompilerExtension extension

    @Override
    void apply(Project project) {
        this.project = project

        project.apply plugin: AsakusaVanillaSdkBasePlugin
        this.extension = AsakusaSdkPlugin.get(project).extensions.create('vanilla', AsakusafwCompilerExtension)

        configureExtension()
        defineTasks()
    }

    private void configureExtension() {
        AsakusaVanillaBaseExtension base = AsakusaVanillaBasePlugin.get(project)
        AsakusafwPluginConvention sdk = AsakusaSdkPlugin.get(project)
        extension.conventionMapping.with {
            outputDirectory = { project.relativePath(new File(project.buildDir, 'vanilla-batchapps')) }
            batchIdPrefix = { (String) 'vanilla.' }
            failOnError = { true }
        }
        extension.compilerProperties.put('javac.version', { sdk.javac.sourceCompatibility.toString() })
        PluginUtils.injectVersionProperty(extension, { base.featureVersion })
    }

    private void defineTasks() {
        AsakusafwPluginConvention sdk = AsakusaSdkPlugin.get(project)
        project.tasks.create(TASK_COMPILE, AsakusaCompileTask) { AsakusaCompileTask task ->
            task.group AsakusaSdkPlugin.ASAKUSAFW_BUILD_GROUP
            task.description 'Compiles Asakusa DSL source files for Asakusa Vanilla'
            task.dependsOn 'classes'

            task.compilerName = 'Asakusa DSL compiler for Vanilla'

            task.launcherClasspath << { project.configurations.asakusaToolLauncher }

            task.toolClasspath << { project.configurations.asakusaVanillaCompiler }
            task.toolClasspath << { project.sourceSets.main.compileClasspath - project.configurations.compile }

            task.explore << { PluginUtils.getClassesDirs(project, project.sourceSets.main.output).findAll { it.exists() } }
            task.embed << { [project.sourceSets.main.output.resourcesDir].findAll { it.exists() } }
            task.attach << { project.configurations.embedded }

            task.include << { extension.include }
            task.exclude << { extension.exclude }

            task.clean = true

            task.conventionMapping.with {
                maxHeapSize = { sdk.maxHeapSize }
                runtimeWorkingDirectory = { extension.runtimeWorkingDirectory }
                batchIdPrefix = { extension.batchIdPrefix }
                outputDirectory = { project.file(extension.outputDirectory) }
                failOnError = { extension.failOnError }
            }
            project.tasks.compileBatchapp.dependsOn task
            project.tasks.jarBatchapp.from { task.outputDirectory }
        }
        PluginUtils.afterEvaluate(project) {
            AsakusaCompileTask task = project.tasks.getByName(TASK_COMPILE)
            Map<String, String> map = [:]
            map.putAll(ResolutionUtils.resolveToStringMap(extension.compilerProperties))
            map.putAll(ResolutionUtils.resolveToStringMap(task.compilerProperties))
            task.compilerProperties = map

            if (sdk.logbackConf != null) {
                File f = project.file(sdk.logbackConf)
                task.systemProperties.put('logback.configurationFile', f.absolutePath)
            }
        }
    }
}
