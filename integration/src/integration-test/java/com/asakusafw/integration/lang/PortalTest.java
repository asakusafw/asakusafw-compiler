/**
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
package com.asakusafw.integration.lang;

import static com.asakusafw.integration.lang.Util.*;

import org.junit.ClassRule;
import org.junit.Test;

import com.asakusafw.integration.AsakusaConfigurator;
import com.asakusafw.integration.AsakusaConstants;
import com.asakusafw.integration.AsakusaProjectProvider;
import com.asakusafw.utils.gradle.Bundle;
import com.asakusafw.utils.gradle.ContentsConfigurator;

/**
 * Test for the portal command.
 */
public class PortalTest {

    /**
     * project provider.
     */
    @ClassRule
    public static final AsakusaProjectProvider PROVIDER = new AsakusaProjectProvider()
            .withProject(ContentsConfigurator.copy(data("vanilla")))
            .withProject(ContentsConfigurator.copy(data("ksv")))
            .withProject(ContentsConfigurator.copy(data("logback-test")))
            .withProject(AsakusaConfigurator.projectHome())
            .withProject(AsakusaConfigurator.hadoop(AsakusaConfigurator.Action.UNSET_ALWAYS))
            .withProvider(provider -> {
                // install framework only once
                framework = provider.newInstance("inf")
                        .gradle("attachVanillaBatchapps", "installAsakusafw")
                        .getFramework();
            });

    static Bundle framework;

    /**
     * bare portal command.
     */
    @Test
    public void command() {
        framework.withLaunch(AsakusaConstants.CMD_PORTAL);
    }

    /**
     * {@code list}.
     */
    @Test
    public void list() {
        framework.withLaunch(AsakusaConstants.CMD_PORTAL, "list");
    }

    /**
     * {@code list batch}.
     */
    @Test
    public void list_batch() {
        framework.withLaunch(AsakusaConstants.CMD_PORTAL, "list", "batch", "-v");
    }

    /**
     * {@code list jobflow}.
     */
    @Test
    public void list_jobflow() {
        framework.withLaunch(AsakusaConstants.CMD_PORTAL, "list", "jobflow", "-v",
                "vanilla.perf.average.sort");
    }

    /**
     * {@code list plan}.
     */
    @Test
    public void list_plan() {
        framework.withLaunch(AsakusaConstants.CMD_PORTAL, "list", "plan", "-v",
                "vanilla.perf.average.sort");
    }

    /**
     * {@code list operator}.
     */
    @Test
    public void list_operator() {
        framework.withLaunch(AsakusaConstants.CMD_PORTAL, "list", "operator", "-v",
                "vanilla.perf.average.sort");
    }

    /**
     * {@code list directio}.
     */
    @Test
    public void list_directio() {
        framework.withLaunch(AsakusaConstants.CMD_PORTAL, "list", "directio");
    }

    /**
     * {@code list directio input}.
     */
    @Test
    public void list_directio_input() {
        framework.withLaunch(AsakusaConstants.CMD_PORTAL, "list", "directio", "input", "-v",
                "vanilla.perf.average.sort");
    }

    /**
     * {@code list directio output}.
     */
    @Test
    public void list_directio_output() {
        framework.withLaunch(AsakusaConstants.CMD_PORTAL, "list", "directio", "output", "-v",
                "vanilla.perf.average.sort");
    }

    /**
     * {@code list windgate}.
     */
    @Test
    public void list_windgate() {
        framework.withLaunch(AsakusaConstants.CMD_PORTAL, "list", "windgate");
    }

    /**
     * {@code list windgate input}.
     */
    @Test
    public void list_windgate_input() {
        framework.withLaunch(AsakusaConstants.CMD_PORTAL, "list", "windgate", "input", "-v",
                "vanilla.wg.perf.average.sort");
    }

    /**
     * {@code list windgate output}.
     */
    @Test
    public void list_windgate_output() {
        framework.withLaunch(AsakusaConstants.CMD_PORTAL, "list", "windgate", "output", "-v",
                "vanilla.wg.perf.average.sort");
    }

    /**
     * {@code list hive}.
     */
    @Test
    public void list_hive() {
        framework.withLaunch(AsakusaConstants.CMD_PORTAL, "list", "hive");
    }

    /**
     * {@code draw}.
     */
    @Test
    public void draw() {
        framework.withLaunch(AsakusaConstants.CMD_PORTAL, "draw");
    }

    /**
     * {@code draw jobflow}.
     */
    @Test
    public void draw_jobflow() {
        framework.withLaunch(AsakusaConstants.CMD_PORTAL, "draw", "jobflow", "-v",
                "vanilla.perf.average.sort");
    }

    /**
     * {@code draw plan}.
     */
    @Test
    public void draw_plan() {
        framework.withLaunch(AsakusaConstants.CMD_PORTAL, "draw", "plan", "-v",
                "vanilla.perf.average.sort");
    }

    /**
     * {@code draw operator}.
     */
    @Test
    public void draw_operator() {
        framework.withLaunch(AsakusaConstants.CMD_PORTAL, "draw", "operator", "-v",
                "vanilla.perf.average.sort");
    }
}
