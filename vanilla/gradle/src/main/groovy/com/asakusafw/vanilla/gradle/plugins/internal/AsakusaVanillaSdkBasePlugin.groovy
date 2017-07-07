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

import org.gradle.api.Plugin
import org.gradle.api.Project

import com.asakusafw.gradle.plugins.AsakusafwSdkExtension
import com.asakusafw.gradle.plugins.AsakusafwSdkPluginParticipant
import com.asakusafw.gradle.plugins.internal.AsakusaSdkPlugin
import com.asakusafw.gradle.plugins.internal.PluginUtils
import com.asakusafw.lang.gradle.plugins.internal.AsakusaLangSdkPlugin

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

        project.apply plugin: AsakusaLangSdkPlugin
        project.apply plugin: AsakusaVanillaBasePlugin

        configureTestkit()
        configureConfigurations()
    }

    private void configureTestkit() {
        AsakusafwSdkExtension sdk = AsakusaSdkPlugin.get(project).sdk
        sdk.availableTestkits << new AsakusaVanillaTestkit(sdk)
    }

    private void configureConfigurations() {
        project.configurations {
            asakusaVanillaCommon {
                description 'Common libraries of Asakusa DSL Compiler for Vanilla'
                extendsFrom project.configurations.asakusaLangCommon
                exclude group: 'asm', module: 'asm'
            }
            asakusaVanillaCompiler {
                description 'Full classpath of Asakusa DSL Compiler for Vanilla'
                extendsFrom project.configurations.asakusaLangCompiler
                extendsFrom project.configurations.asakusaVanillaCommon
            }
            asakusaVanillaTestkit {
                description 'Asakusa DSL testkit classpath for Vanilla'
                extendsFrom project.configurations.asakusaLangTestkit
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

                    if (features.directio) {
                        asakusaVanillaCommon "com.asakusafw.dag.compiler:asakusa-dag-compiler-extension-directio:${base.featureVersion}"
                    }
                    if (features.windgate) {
                        asakusaVanillaCommon "com.asakusafw.dag.compiler:asakusa-dag-compiler-extension-windgate:${base.featureVersion}"
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

    /**
     * A participant descriptor for {@link AsakusaVanillaSdkBasePlugin}.
     * @since 0.9.2
     */
    static class Participant implements AsakusafwSdkPluginParticipant {

        @Override
        String getName() {
            return descriptor.simpleName
        }

        @Override
        Class<?> getDescriptor() {
            return AsakusaVanillaSdkBasePlugin
        }
    }
}
