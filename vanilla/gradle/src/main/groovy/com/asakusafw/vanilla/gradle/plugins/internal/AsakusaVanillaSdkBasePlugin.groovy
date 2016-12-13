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

import com.asakusafw.gradle.plugins.AsakusafwSdkExtension
import com.asakusafw.gradle.plugins.internal.AsakusaSdkPlugin
import com.asakusafw.gradle.plugins.internal.PluginUtils

/**
 * A base plug-in of {@link AsakusaVanillaSdkPlugin}.
 * This only organizes dependencies and testkits.
 * @since 0.4.0
 */
class AsakusaVanillaSdkBasePlugin implements Plugin<Project> {

    private Project project

    @Override
    void apply(Project project) {
        this.project = project

        project.apply plugin: AsakusaSdkPlugin
        project.apply plugin: AsakusaVanillaBasePlugin

        configureTestkit()
        configureConfigurations()
    }

    private void configureTestkit() {
        AsakusafwSdkExtension sdk = AsakusaSdkPlugin.get(project).sdk
        sdk.availableTestkits << new AsakusaVanillaTestkit()
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
            AsakusafwSdkExtension features = AsakusaSdkPlugin.get(project).sdk
            project.dependencies {
                if (features.core) {
                    asakusaVanillaCommon "com.asakusafw.vanilla.compiler:asakusa-vanilla-compiler-core:${base.featureVersion}"
                    asakusaVanillaCommon "com.asakusafw.lang.compiler:asakusa-compiler-extension-cleanup:${base.featureVersion}"
                    asakusaVanillaCommon "com.asakusafw.lang.compiler:asakusa-compiler-extension-redirector:${base.featureVersion}"
                    asakusaVanillaCommon "com.asakusafw.lang.compiler:asakusa-compiler-extension-yaess:${base.featureVersion}"
                    asakusaVanillaCommon "com.asakusafw.lang.compiler:asakusa-compiler-cli:${base.featureVersion}"
                    asakusaVanillaCommon "com.asakusafw:simple-graph:${base.coreVersion}"
                    asakusaVanillaCommon "com.asakusafw:java-dom:${base.coreVersion}"
                    asakusaVanillaCompiler "com.asakusafw:asakusa-dsl-vocabulary:${base.coreVersion}"
                    asakusaVanillaCompiler "com.asakusafw:asakusa-runtime:${base.coreVersion}"
                    asakusaVanillaCompiler "com.asakusafw:asakusa-yaess-core:${base.coreVersion}"

                    if (features.directio) {
                        asakusaVanillaCommon "com.asakusafw.dag.compiler:asakusa-dag-compiler-extension-directio:${base.featureVersion}"
                        asakusaVanillaCompiler "com.asakusafw:asakusa-directio-vocabulary:${base.coreVersion}"
                    }
                    if (features.windgate) {
                        asakusaVanillaCommon "com.asakusafw.dag.compiler:asakusa-dag-compiler-extension-windgate:${base.featureVersion}"
                        asakusaVanillaCompiler "com.asakusafw:asakusa-windgate-vocabulary:${base.coreVersion}"
                    }
                    if (features.hive) {
                        asakusaVanillaCommon "com.asakusafw.lang.compiler:asakusa-compiler-extension-hive:${base.featureVersion}"
                    }
                }
                if (features.testing) {
                    asakusaVanillaTestkit "com.asakusafw.vanilla.runtime:asakusa-vanilla-assembly:${base.featureVersion}"
                    asakusaVanillaTestkit "com.asakusafw.vanilla.testkit:asakusa-vanilla-test-adapter:${base.featureVersion}"
                    asakusaVanillaTestkit "com.asakusafw.vanilla.testkit:asakusa-vanilla-test-inprocess:${base.featureVersion}"
                    asakusaVanillaTestkit "com.asakusafw:asakusa-test-inprocess:${base.coreVersion}"
                    asakusaVanillaTestkit "com.asakusafw:asakusa-test-windows:${base.coreVersion}"

                    if (features.windgate) {
                        asakusaVanillaTestkit "com.asakusafw:asakusa-windgate-test-inprocess:${base.coreVersion}"
                    }
                }
            }
        }
    }
}
