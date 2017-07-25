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

import org.gradle.api.Plugin
import org.gradle.api.Project

import com.asakusafw.gradle.plugins.AsakusafwSdkExtension
import com.asakusafw.gradle.plugins.internal.AsakusaSdkPlugin
import com.asakusafw.gradle.plugins.internal.PluginUtils

/**
 * A Gradle sub plug-in for Asakusa Language SDK.
 * @since 0.4.2
 */
class AsakusaLangSdkPlugin implements Plugin<Project> {

    private Project project

    @Override
    void apply(Project project) {
        this.project = project

        project.apply plugin: AsakusaSdkPlugin
        project.apply plugin: AsakusaLangBasePlugin

        configureConfigurations()
    }

    private void configureConfigurations() {
        project.configurations {
            asakusaLangCommon {
                description 'Common libraries of Asakusa DSL Compiler'
                exclude group: 'asm', module: 'asm'
            }
            asakusaLangCompiler {
                description 'Full classpath of Asakusa DSL Compiler'
                extendsFrom project.configurations.compile
                extendsFrom project.configurations.asakusaLangCommon
            }
            asakusaLangTestkit {
                description 'Asakusa DSL testkit classpath'
                extendsFrom project.configurations.asakusaLangCommon
            }
        }
        PluginUtils.afterEvaluate(project) {
            AsakusaLangBaseExtension base = AsakusaLangBasePlugin.get(project)
            AsakusafwSdkExtension features = AsakusaSdkPlugin.get(project).sdk
            project.dependencies {
                if (features.core) {
                    asakusaLangCommon "com.asakusafw.lang.compiler:asakusa-compiler-cli:${base.featureVersion}"
                    asakusaLangCommon "com.asakusafw.lang.compiler:asakusa-compiler-extension-cleanup:${base.featureVersion}"
                    asakusaLangCommon "com.asakusafw.lang.compiler:asakusa-compiler-extension-redirector:${base.featureVersion}"
                    asakusaLangCommon "com.asakusafw.lang.compiler:asakusa-compiler-extension-yaess:${base.featureVersion}"
                    asakusaLangCommon "com.asakusafw.lang.compiler:asakusa-compiler-extension-info:${base.featureVersion}"
                    asakusaLangCommon "com.asakusafw:simple-graph:${base.coreVersion}"
                    asakusaLangCommon "com.asakusafw:java-dom:${base.coreVersion}"
                    asakusaLangCommon "com.asakusafw:asakusa-yaess-core:${base.coreVersion}"
                    asakusaLangCommon "com.asakusafw.info:asakusa-info-model:${base.coreVersion}"
                    asakusaLangCompiler "com.asakusafw:asakusa-dsl-vocabulary:${base.coreVersion}"
                    asakusaLangCompiler "com.asakusafw:asakusa-runtime:${base.coreVersion}"

                    if (features.directio) {
                        asakusaLangCommon "com.asakusafw.lang.compiler:asakusa-compiler-extension-directio:${base.featureVersion}"
                        asakusaLangCommon "com.asakusafw.info:asakusa-info-directio:${base.coreVersion}"
                        asakusaLangCompiler "com.asakusafw:asakusa-directio-vocabulary:${base.coreVersion}"
                    }
                    if (features.windgate) {
                        asakusaLangCommon "com.asakusafw.lang.compiler:asakusa-compiler-extension-windgate:${base.featureVersion}"
                        asakusaLangCommon "com.asakusafw.info:asakusa-info-windgate:${base.coreVersion}"
                        asakusaLangCompiler "com.asakusafw:asakusa-windgate-vocabulary:${base.coreVersion}"
                    }
                    if (features.hive) {
                        asakusaLangCommon "com.asakusafw.lang.compiler:asakusa-compiler-extension-hive:${base.featureVersion}"
                    }
                }
            }
        }
    }
}
