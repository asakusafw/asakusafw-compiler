/**
 * Copyright 2011-2015 Asakusa Framework Team.
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
package com.asakusafw.lang.compiler.tool.yaess.compress;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.asakusafw.utils.graph.Graph;
import com.asakusafw.utils.graph.Graphs;
import com.asakusafw.yaess.core.BatchScript;
import com.asakusafw.yaess.core.ExecutionPhase;
import com.asakusafw.yaess.core.ExecutionScript;
import com.asakusafw.yaess.core.ExecutionScript.Kind;
import com.asakusafw.yaess.core.FlowScript;
import com.asakusafw.yaess.core.HadoopScript;

/**
 * Compresses YAESS scripts.
 */
public class ScriptCompressor {

    private final BatchScript origin;

    private final BatchScript compressed;

    private final Map<String, List<String>> entries;

    /**
     * Creates a new instance.
     * @param script the target script
     */
    public ScriptCompressor(BatchScript script) {
        this.origin = script;
        Map<String, ScriptInfo> resolved = collect(script);
        this.entries = simplify(resolved);
        this.compressed = compress(script, resolved);
    }

    private static Map<String, ScriptInfo> collect(BatchScript script) {
        Map<String, ScriptInfo> results = new HashMap<>();
        for (FlowScript flow : script.getAllFlows()) {
            ScriptInfo entries = collect(flow);
            if (entries != null) {
                results.put(flow.getId(), entries);
            }
        }
        return results;
    }

    private static ScriptInfo collect(FlowScript flow) {
        Set<ExecutionScript> main = flow.getScripts().get(ExecutionPhase.MAIN);
        if (main == null || main.size() <= 1) {
            return null;
        }
        ScriptInfo info = null;
        Map<String, HadoopScript> map = new HashMap<>();
        for (ExecutionScript script : main) {
            if (script.getKind() != Kind.HADOOP) {
                return null;
            }
            HadoopScript hadoop = (HadoopScript) script;
            if (info == null) {
                info = new ScriptInfo(hadoop);
            } else if (info.accepts(hadoop) == false) {
                return null;
            }
            map.put(script.getId(), hadoop);
        }
        assert info != null;
        Graph<HadoopScript> graph = Graphs.newInstance();
        for (HadoopScript script : map.values()) {
            graph.addNode(script);
            for (String blockerId : script.getBlockerIds()) {
                HadoopScript blocker = map.get(blockerId);
                assert blocker != null;
                graph.addEdge(script, blocker);
            }
        }
        for (HadoopScript script : Graphs.sortPostOrder(graph)) {
            info.classes.add(script.getClassName());
        }
        return info;
    }

    private Map<String, List<String>> simplify(Map<String, ScriptInfo> resolved) {
        Map<String, List<String>> results = new LinkedHashMap<>();
        for (Map.Entry<String, ScriptInfo> entry : resolved.entrySet()) {
            results.put(entry.getKey(), Collections.unmodifiableList(entry.getValue().classes));
        }
        return Collections.unmodifiableMap(results);
    }

    private BatchScript compress(BatchScript script, Map<String, ScriptInfo> targets) {
        if (targets.isEmpty()) {
            return script;
        }
        List<FlowScript> flows = new ArrayList<>();
        for (FlowScript flow : script.getAllFlows()) {
            Map<ExecutionPhase, Set<ExecutionScript>> phases = new EnumMap<>(ExecutionPhase.class);
            for (Map.Entry<ExecutionPhase, Set<ExecutionScript>> entry : flow.getScripts().entrySet()) {
                phases.put(entry.getKey(), new LinkedHashSet<>(entry.getValue()));
            }
            ScriptInfo info = targets.get(flow.getId());
            if (info != null) {
                phases.put(ExecutionPhase.MAIN, Collections.<ExecutionScript>singleton(new HadoopScript(
                        "compressed",
                        Collections.<String>emptySet(),
                        info.className,
                        info.props,
                        info.env)));
            }
            flows.add(new FlowScript(flow.getId(), flow.getBlockerIds(), phases));
        }
        BatchScript copy = new BatchScript(script.getId(), script.getBuildId(), flows);
        return copy;
    }

    /**
     * Returns the original script.
     * @return the original script
     */
    public BatchScript getScript() {
        return origin;
    }

    /**
     * Returns the compressed script.
     * @return the compressed script
     */
    public BatchScript getCompressed() {
        return compressed;
    }

    /**
     * Returns compressed clients for each jobflow.
     * @return the compressed clients
     */
    public Map<String, List<String>> getEntries() {
        return entries;
    }

    private static class ScriptInfo {

        final String className = Util.getClientClassName();

        final Map<String, String> env;

        final Map<String, String> props;

        final List<String> classes = new ArrayList<>();

        public ScriptInfo(HadoopScript hadoop) {
            this.env = hadoop.getEnvironmentVariables();
            this.props = hadoop.getHadoopProperties();
        }

        public boolean accepts(HadoopScript hadoop) {
            return hadoop.getEnvironmentVariables().equals(env)
                    && hadoop.getHadoopProperties().equals(props);
        }
    }
}
