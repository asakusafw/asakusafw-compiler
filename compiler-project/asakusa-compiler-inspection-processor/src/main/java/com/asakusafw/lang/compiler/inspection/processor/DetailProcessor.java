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
import java.util.Map;

import com.asakusafw.lang.compiler.inspection.InspectionNode;
import com.asakusafw.lang.compiler.inspection.InspectionNode.Port;
import com.asakusafw.lang.compiler.inspection.InspectionNode.PortReference;

/**
 * Generates text based node detail.
 */
public class DetailProcessor implements InspectionNodeProcessor {

    static final Charset ENCODING = Charset.forName("UTF-8");

    @Override
    public void process(Context context, InspectionNode node, OutputStream output) throws IOException {
        try (Editor editor = new Editor(context, output)) {
            editor.put("id: {0}", node.getId());
            editor.put("title: {0}", node.getTitle());
            processProperties(editor, node.getProperties());

            processPorts(editor, "inputs", node.getInputs().values());
            processPorts(editor, "outputs", node.getOutputs().values());

            processNodes(editor, node.getElements().values());
        }
    }

    private void processProperties(Editor editor, Map<String, String> properties) {
        editor.put("properties:");
        editor.push();
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            editor.put("{0}: {1}", entry.getKey(), entry.getValue());
        }
        editor.pop();
    }

    private void processPorts(Editor editor, String label, Iterable<Port> ports) {
        editor.put("{0}:", label);
        editor.push();
        for (Port port : ports) {
            editor.put("{0}:", port.getId());
            editor.push();
            processProperties(editor, port.getProperties());

            editor.put("opposites:");
            editor.push();
            for (PortReference opposite : port.getOpposites()) {
                editor.put("{0}.{1}", opposite.getNodeId(), opposite.getPortId());
            }
            editor.pop();
            editor.pop();
        }
        editor.pop();
    }

    private void processNodes(Editor editor, Collection<InspectionNode> elements) {
        editor.put("elements:");
        editor.push();
        for (InspectionNode element : elements) {
            editor.put("{0} ({1})", element.getId(), element.getTitle());
        }
        editor.pop();
    }

    private static class Editor implements Closeable {

        private final PrintWriter writer;

        private int indent = 0;

        Editor(Context context, OutputStream output) {
            this.writer = new PrintWriter(new OutputStreamWriter(output, ENCODING));
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
}
