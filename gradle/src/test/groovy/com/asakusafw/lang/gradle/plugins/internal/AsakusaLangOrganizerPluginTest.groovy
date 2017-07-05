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
import org.gradle.testfixtures.ProjectBuilder
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TestRule
import org.junit.runner.Description
import org.junit.runners.model.Statement

import com.asakusafw.gradle.plugins.AsakusafwOrganizerPluginConvention

/**
 * Test for {@link AsakusaLangOrganizerPlugin}.
 */
class AsakusaLangOrganizerPluginTest {

    /**
     * The test initializer.
     */
    @Rule
    public final TestRule initializer = new TestRule() {
        Statement apply(Statement stmt, Description desc) {
            project = ProjectBuilder.builder().withName(desc.methodName).build()
            project.apply plugin: AsakusaLangOrganizerPlugin
            return stmt
        }
    }

    Project project

    /**
     * test for base plug-ins.
     */
    @Test
    void base() {
        assert project.plugins.hasPlugin('asakusafw-organizer') != null
        assert project.plugins.hasPlugin(AsakusaLangBasePlugin) != null
    }

    /**
     * test for {@code tasks.attachComponentLang_*}.
     */
    @Test
    void tasks_attachComponentLang() {
        assert project.tasks.findByName('attachComponentLangTools_dev') != null
        assert project.tasks.findByName('attachComponentLangTools_prod') != null

        assert project.tasks.findByName('attachComponentLangTools_testing') == null
        project.asakusafwOrganizer.profiles.testing {
            // ok
        }
        assert project.tasks.findByName('attachComponentLangTools_testing') != null
    }
}
