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

import org.gradle.api.Plugin
import org.gradle.api.Project

import com.asakusafw.gradle.plugins.AsakusafwBasePlugin
import com.asakusafw.gradle.plugins.AsakusafwCompilerExtension
import com.asakusafw.gradle.plugins.AsakusafwPluginConvention
import com.asakusafw.gradle.plugins.internal.AsakusaSdkPlugin
import com.asakusafw.gradle.plugins.internal.PluginUtils
import com.asakusafw.gradle.tasks.AsakusaCompileTask
import com.asakusafw.gradle.tasks.internal.ResolutionUtils

/**
 * A Gradle sub plug-in for Asakusa Vanilla SDK.
 * @since 0.4.0
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

        project.apply plugin: 'asakusafw-sdk'
        project.apply plugin: AsakusaVanillaBasePlugin
        extension = AsakusaSdkPlugin.get(project).extensions.create('vanilla', AsakusafwCompilerExtension)

        configureExtension()
        configureConfigurations()
        defineTasks()
    }

    private void configureExtension() {
        AsakusafwPluginConvention sdk = AsakusaSdkPlugin.get(project)
        extension.conventionMapping.with {
            outputDirectory = { project.relativePath(new File(project.buildDir, 'vanilla-batchapps')) }
            batchIdPrefix = { (String) 'vanilla.' }
            failOnError = { true }
        }
        extension.compilerProperties.put('javac.version', { sdk.javac.sourceCompatibility.toString() })
    }

    private void configureConfigurations() {
        project.configurations {
            asakusaVanillaCommon {
                description 'Common libraries of Asakusa DSL Compiler for Vanilla'
                exclude group: 'asm', module: 'asm'
            }
            asakusaVanillaCompiler {
                description 'Full classpath of Asakusa DSL Compiler for Vanilla'
                extendsFrom project.configurations.compile
                extendsFrom project.configurations.asakusaVanillaCommon
            }
            asakusaVanillaTestkit {
                description 'Asakusa DSL testkit classpath for Vanilla'
                extendsFrom project.configurations.asakusaVanillaCommon
                exclude group: 'com.asakusafw', module: 'asakusa-test-mapreduce'
            }
        }
        PluginUtils.afterEvaluate(project) {
            AsakusaVanillaBaseExtension base = AsakusaVanillaBasePlugin.get(project)
            AsakusafwPluginConvention sdk = AsakusaSdkPlugin.get(project)
            project.dependencies {
                asakusaVanillaCommon "com.asakusafw.vanilla.compiler:asakusa-vanilla-compiler-core:${base.featureVersion}"
                asakusaVanillaCommon "com.asakusafw.lang.compiler:asakusa-compiler-extension-redirector:${base.featureVersion}"
                asakusaVanillaCommon "com.asakusafw.lang.compiler:asakusa-compiler-extension-yaess:${base.featureVersion}"
                asakusaVanillaCommon "com.asakusafw:simple-graph:${sdk.asakusafwVersion}"
                asakusaVanillaCommon "com.asakusafw:java-dom:${sdk.asakusafwVersion}"
                asakusaVanillaCommon "com.asakusafw.lang.compiler:asakusa-compiler-cli:${base.featureVersion}"
                asakusaVanillaCompiler "com.asakusafw:asakusa-dsl-vocabulary:${sdk.asakusafwVersion}"
                asakusaVanillaCompiler "com.asakusafw:asakusa-runtime:${sdk.asakusafwVersion}"
                asakusaVanillaCompiler "com.asakusafw:asakusa-yaess-core:${sdk.asakusafwVersion}"

                asakusaVanillaCommon "com.asakusafw.dag.compiler:asakusa-dag-compiler-extension-directio:${base.featureVersion}"
                asakusaVanillaCompiler "com.asakusafw:asakusa-directio-vocabulary:${sdk.asakusafwVersion}"

                asakusaVanillaCommon "com.asakusafw.dag.compiler:asakusa-dag-compiler-extension-windgate:${base.featureVersion}"
                asakusaVanillaCompiler "com.asakusafw:asakusa-windgate-vocabulary:${sdk.asakusafwVersion}"

                asakusaVanillaCommon "com.asakusafw.lang.compiler:asakusa-compiler-extension-hive:${base.featureVersion}"

                asakusaVanillaTestkit "com.asakusafw.vanilla.runtime:asakusa-vanilla-assembly:${base.featureVersion}"
                asakusaVanillaTestkit "com.asakusafw.vanilla.testkit:asakusa-vanilla-test-adapter:${base.featureVersion}"
                asakusaVanillaTestkit "com.asakusafw.vanilla.testkit:asakusa-vanilla-test-inprocess:${base.featureVersion}"
                asakusaVanillaTestkit "com.asakusafw:asakusa-test-inprocess:${sdk.asakusafwVersion}"
            }
        }
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

            task.explore << { [project.sourceSets.main.output.classesDir].findAll { it.exists() } }
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
        extendVersionsTask()
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

    private void extendVersionsTask() {
        project.tasks.getByName(AsakusafwBasePlugin.TASK_VERSIONS) << {
            logger.lifecycle "Vanilla: ${AsakusaVanillaBasePlugin.get(project).featureVersion}"
        }
    }
}
