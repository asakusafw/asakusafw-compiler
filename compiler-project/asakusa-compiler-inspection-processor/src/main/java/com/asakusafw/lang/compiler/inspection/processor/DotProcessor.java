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
package com.asakusafw.lang.compiler.inspection.processor;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.text.MessageFormat;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.asakusafw.lang.compiler.inspection.InspectionNode;

/**
 * Generates Graphviz DOT script for elements in the {@link InspectionNode}.
 */
public class DotProcessor implements InspectionNodeProcessor {

    /**
     * Prints verbose information for each node.
     */
    public static final String KEY_VERBOSE = "verbose"; //$NON-NLS-1$

    static final Charset ENCODING = Charset.forName("UTF-8");

    @Override
    public void process(Context context, InspectionNode node, OutputStream output) throws IOException {
        boolean verbose = context.getOption(KEY_VERBOSE, false);
        process(node, output, verbose);
    }

    /**
     * Generates Graphviz DOT script.
     * @param node the target node
     * @param output the target output
     * @param verbose {@code true} to output detail graph
     * @throws IOException if failed to generate the script
     */
    public static void process(InspectionNode node, OutputStream output, boolean verbose) throws IOException {
        try (Editor editor = new Editor(output, verbose)) {
            editor.put("digraph {0} '{'", literal(node.getId()));
            editor.push();
            editor.put("label = {0};", literal(node.getId()));
            Map<String, InspectionNode> elements = node.getElements();
            for (InspectionNode element : elements.values()) {
                editor.put(element);
            }
            for (InspectionNode element : elements.values()) {
                for (InspectionNode.Port port : element.getOutputs().values()) {
                    for (InspectionNode.PortReference opposite : port.getOpposites()) {
                        InspectionNode oppositeNode = elements.get(opposite.getNodeId());
                        if (oppositeNode != null) {
                            editor.connect(element, port.getId(), oppositeNode, opposite.getPortId());
                        }
                    }
                }
            }
            editor.pop();
            editor.put("'}'");
        }
    }

    static String literal(String string) {
        StringBuilder buf = new StringBuilder();
        buf.append('"');
        for (char c : string.toCharArray()) {
            if (c == '\\' || c == '"') {
                buf.append('\\');
                buf.append(c);
            } else if (c == '\n') {
                buf.append('\\');
                buf.append('n');
            } else {
                buf.append(c);
            }
        }
        buf.append('"');
        return buf.toString();
    }

    private static class Editor implements Closeable {

        private static final String PREFIX_IN = "in.";

        private static final String PREFIX_OUT = "out.";

        private final PrintWriter writer;

        private final boolean verbose;

        private final Set<NodeArc> sawNodeArcs = new HashSet<>();

        private int indent = 0;

        Editor(OutputStream output, boolean verbose) {
            this.writer = new PrintWriter(new OutputStreamWriter(output, ENCODING));
            this.verbose = verbose;
        }

        void push() {
            indent++;
        }

        void pop() {
            if (indent == 0) {
                throw new IllegalStateException();
            }
            indent--;
        }

        public void put(InspectionNode node) {
            if (verbose) {
                putVerbose(node);
            } else {
                putSimple(node);
            }
        }

        public void connect(
                InspectionNode upstreamNode, String upstreamPort,
                InspectionNode downstreamNode, String downstreamPort) {
            if (verbose) {
                connectVerbose(upstreamNode, upstreamPort, downstreamNode, downstreamPort);
            } else {
                connectSimple(upstreamNode, downstreamNode);
            }
        }

        private void putSimple(InspectionNode node) {
            StringBuilder buf = new StringBuilder();
            buf.append(node.getTitle());
            buf.append('\n');
            buf.append('@');
            buf.append(node.getId());
            put("\"{0}\" [shape = box, label = {1}];",
                    node.getId(),
                    literal(buf.toString()));
        }

        private void putVerbose(InspectionNode node) {
            StringBuilder buf = new StringBuilder();
            buf.append('{');
            if (node.getInputs().isEmpty() == false) {
                appendPorts(buf, PREFIX_IN, node.getInputs().values());
                buf.append('|');
            }
            buf.append(getNodeLabel(node));
            if (node.getOutputs().isEmpty() == false) {
                buf.append('|');
                appendPorts(buf, PREFIX_OUT, node.getOutputs().values());
            }
            buf.append('}');
            put("\"{0}\" [shape = record, label = {1}];", node.getId(), literal(buf.toString()));
        }

        private void appendPorts(StringBuilder buf, String prefix, Collection<InspectionNode.Port> ports) {
            assert ports.isEmpty() == false;
            buf.append('{');
            int index = 0;
            for (InspectionNode.Port port : ports) {
                if (index++ != 0) {
                    buf.append('|');
                }
                appendPort(buf, prefix, port);
            }
            buf.append('}');
        }

        private void appendPort(StringBuilder buf, String prefix, InspectionNode.Port port) {
            buf.append('<');
            buf.append(prefix);
            buf.append(port.getId());
            buf.append('>');
            buf.append(' ');
            buf.append(getPortLabel(port));
        }

        private String getNodeLabel(InspectionNode node) {
            StringBuilder buf = new StringBuilder();
            buf.append(String.format("%s [@%s]", node.getTitle(), node.getId()));
            appendProperties(buf, node.getProperties());
            return escapeForRecord(buf);
        }

        private String getPortLabel(InspectionNode.Port port) {
            StringBuilder buf = new StringBuilder();
            buf.append(port.getId());
            appendProperties(buf, port.getProperties());
            return escapeForRecord(buf);
        }

        private void appendProperties(StringBuilder buf, Map<String, String> properties) {
            for (Map.Entry<String, String> entry : properties.entrySet()) {
                buf.append('\n');
                buf.append(String.format("%s: %s", entry.getKey(), entry.getValue())); //$NON-NLS-1$
            }
        }

        private String escapeForRecord(CharSequence string) {
            StringBuilder buf = new StringBuilder();
            for (int i = 0, n = string.length(); i < n; i++) {
                char c = string.charAt(i);
                if (c == '{' || c == '<') {
                    buf.append('(');
                } else if (c == '}' || c == '>') {
                    buf.append(')');
                } else if (c == '|') {
                    buf.append('/');
                } else {
                    buf.append(c);
                }
            }
            return buf.toString();
        }

        private void connectSimple(InspectionNode upstreamNode, InspectionNode downstreamNode) {
            NodeArc arc = new NodeArc(upstreamNode.getId(), downstreamNode.getId());
            if (sawNodeArcs.contains(arc) == false) {
                sawNodeArcs.add(arc);
                put("{0} -> {1};",
                        literal(upstreamNode.getId()), literal(downstreamNode.getId()));
            }
        }

        private void connectVerbose(
                InspectionNode upstreamNode, String upstreamPort,
                InspectionNode downstreamNode, String downstreamPort) {
            put("{0}:{1} -> {2}:{3}",
                    literal(upstreamNode.getId()), literal(PREFIX_OUT + upstreamPort),
                    literal(downstreamNode.getId()), literal(PREFIX_IN + downstreamPort));
        }

        void put(String pattern, Object... arguments) {
            assert pattern != null;
            assert arguments != null;
            StringBuilder buf = new StringBuilder();
            for (int i = 0, n = indent; i < n; i++) {
                buf.append("    ");
            }
            buf.append(MessageFormat.format(pattern, arguments));
            String text = buf.toString();
            writer.println(text);
        }

        @Override
        public void close() throws IOException {
            writer.flush();
        }
    }

    private static class NodeArc {

        final String sourceId;

        final String destinationId;

        public NodeArc(String sourceId, String destinationId) {
            this.sourceId = sourceId;
            this.destinationId = destinationId;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + sourceId.hashCode();
            result = prime * result + destinationId.hashCode();
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            NodeArc other = (NodeArc) obj;
            if (!sourceId.equals(other.sourceId)) {
                return false;
            }
            if (!destinationId.equals(other.destinationId)) {
                return false;
            }
            return true;
        }
    }
}
