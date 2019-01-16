/**
 * Copyright 2011-2019 Asakusa Framework Team.
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
package com.asakusafw.lang.inspection.processor;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.text.MessageFormat;
import java.util.Collection;
import java.util.Map;

import com.asakusafw.lang.inspection.InspectionNode;
import com.asakusafw.lang.inspection.InspectionNode.Port;
import com.asakusafw.lang.inspection.InspectionNode.PortReference;

/**
 * Generates text based node detail.
 */
public class DetailProcessor implements InspectionNodeProcessor {

    static final Charset ENCODING = StandardCharsets.UTF_8;

    @Override
    public void process(Context context, InspectionNode node, OutputStream output) throws IOException {
        try (Editor editor = new Editor(context, output)) {
            editor.put("id: {0}", node.getId()); //$NON-NLS-1$
            editor.put("title: {0}", node.getTitle()); //$NON-NLS-1$
            processProperties(editor, node.getProperties());

            processPorts(editor, "inputs", node.getInputs().values()); //$NON-NLS-1$
            processPorts(editor, "outputs", node.getOutputs().values()); //$NON-NLS-1$

            processNodes(editor, node.getElements().values());
        }
    }

    private void processProperties(Editor editor, Map<String, String> properties) {
        editor.put("properties:"); //$NON-NLS-1$
        editor.push();
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            editor.put("{0}: {1}", entry.getKey(), entry.getValue()); //$NON-NLS-1$
        }
        editor.pop();
    }

    private void processPorts(Editor editor, String label, Iterable<Port> ports) {
        editor.put("{0}:", label); //$NON-NLS-1$
        editor.push();
        for (Port port : ports) {
            editor.put("{0}:", port.getId()); //$NON-NLS-1$
            editor.push();
            processProperties(editor, port.getProperties());

            editor.put("opposites:"); //$NON-NLS-1$
            editor.push();
            for (PortReference opposite : port.getOpposites()) {
                editor.put("{0}.{1}", opposite.getNodeId(), opposite.getPortId()); //$NON-NLS-1$
            }
            editor.pop();
            editor.pop();
        }
        editor.pop();
    }

    private void processNodes(Editor editor, Collection<InspectionNode> elements) {
        editor.put("elements:"); //$NON-NLS-1$
        editor.push();
        for (InspectionNode element : elements) {
            editor.put("{0} ({1})", element.getId(), element.getTitle()); //$NON-NLS-1$
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
                buf.append("    "); //$NON-NLS-1$
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
