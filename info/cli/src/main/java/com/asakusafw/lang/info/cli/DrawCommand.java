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
package com.asakusafw.lang.info.cli;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

import io.airlift.airline.Option;

/**
 * An abstract implementation of drawing commands.
 * @since 0.4.2
 */
public abstract class DrawCommand extends SingleJobflowInfoCommand {

    @Option(
            name = { "--concentrate", },
            title = "concentrate multi-edges",
            description = "concentrate multi-edges",
            arity = 0,
            required = false)
    Boolean concentrate;

    /**
     * Returns the graph options.
     * @return the graph options
     */
    protected Map<String, ?> getGraphOptions() {
        Map<String, String> graphs = new LinkedHashMap<>();
        put(graphs, "compound", true);
        put(graphs, "concentrate", concentrate);

        Map<String, String> nodes = new LinkedHashMap<>();
        Map<String, String> edges = new LinkedHashMap<>();

        Map<String, Object> results = new LinkedHashMap<>();
        results.put("graph", graphs);
        results.put("node", nodes);
        results.put("edge", edges);
        return results;
    }

    private static void put(Map<String, String> map, String key, Object value) {
        Optional.ofNullable(value).ifPresent(it -> map.put(key, String.valueOf(it)));
    }
}
