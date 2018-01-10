/*
 * Copyright 2011-2018 Asakusa Framework Team.
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
import org.gradle.testfixtures.ProjectBuilder
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TestRule
import org.junit.runner.Description
import org.junit.runners.model.Statement

import com.asakusafw.gradle.plugins.AsakusafwOrganizerPluginConvention
import com.asakusafw.lang.gradle.plugins.internal.AsakusaLangOrganizerPlugin
import com.asakusafw.vanilla.gradle.plugins.AsakusafwOrganizerVanillaExtension

/**
 * Test for {@link AsakusaVanillaOrganizerPlugin}.
 */
class AsakusaVanillaOrganizerPluginTest {

    /**
     * The test initializer.
     */
    @Rule
    public final TestRule initializer = new TestRule() {
        Statement apply(Statement stmt, Description desc) {
            project = ProjectBuilder.builder().withName(desc.methodName).build()
            project.apply plugin: AsakusaVanillaOrganizerPlugin
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
        assert project.plugins.hasPlugin(AsakusaVanillaBasePlugin) != null
        assert project.plugins.hasPlugin(AsakusaLangOrganizerPlugin) != null
    }

    /**
     * test for extension.
     */
    @Test
    void extension() {
        AsakusafwOrganizerPluginConvention root = project.asakusafwOrganizer
        AsakusafwOrganizerVanillaExtension extension = root.vanilla
        assert extension != null

        assert extension.enabled == true

        assert root.profiles.dev.vanilla.enabled == true
        assert root.profiles.prod.vanilla.enabled == true

        root.profiles.testing {
            // ok
        }
        assert root.profiles.testing.vanilla.enabled == true
    }

    /**
     * Test for {@code project.asakusafwOrganizer.vanilla.version}.
     */
    @Test
    void extension_version() {
        project.asakusaVanillaBase.featureVersion = '__VERSION__'
        assert project.asakusafwOrganizer.vanilla.version == '__VERSION__'
        assert project.asakusafwOrganizer.profiles.dev.vanilla.version == '__VERSION__'
        assert project.asakusafwOrganizer.profiles.prod.vanilla.version == '__VERSION__'
        assert project.asakusafwOrganizer.profiles.other.vanilla.version == '__VERSION__'
    }

    /**
     * test for extension.
     */
    @Test
    void extension_inherited() {
        AsakusafwOrganizerPluginConvention root = project.asakusafwOrganizer
        AsakusafwOrganizerVanillaExtension extension = root.vanilla

        extension.enabled = false

        assert root.profiles.dev.vanilla.enabled == false
        assert root.profiles.prod.vanilla.enabled == false

        root.profiles.prod.vanilla.enabled = true
        assert extension.enabled == false
        assert root.profiles.dev.vanilla.enabled == false
        assert root.profiles.prod.vanilla.enabled == true

        root.profiles.testing {
            // ok
        }
        assert root.profiles.testing.vanilla.enabled == false
    }

    /**
     * test for {@code tasks.attachComponentVanilla_*}.
     */
    @Test
    void tasks_attachComponentVanilla() {
        assert project.tasks.findByName('attachComponentVanilla_dev') != null
        assert project.tasks.findByName('attachComponentVanilla_prod') != null

        assert project.tasks.findByName('attachComponentVanilla_testing') == null
        project.asakusafwOrganizer.profiles.testing {
            // ok
        }
        assert project.tasks.findByName('attachComponentVanilla_testing') != null
    }
}
