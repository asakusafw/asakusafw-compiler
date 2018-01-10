/**
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
package com.asakusafw.lang.inspection.gui;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.ProcessBuilder.Redirect;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import com.asakusafw.lang.inspection.InspectionNode;
import com.asakusafw.lang.inspection.processor.DotProcessor;

/**
 * Exports inspection objects as PDF file.
 */
public class PdfExporter {

    /**
     * the system property key of Graphviz DOT location.
     */
    public static final String KEY_DOT = "dot"; //$NON-NLS-1$

    private final File dot;

    /**
     * Creates a new instance.
     * @param dot the path or name to the Graphviz DOT executable
     */
    public PdfExporter(File dot) {
        this.dot = dot;
    }

    /**
     * Finds a suitable PDF exporter.
     * @return the PDF exporter, or {@code null} if there is no such a PDF exporter
     */
    public static PdfExporter find() {
        File exec = findExecutable();
        if (exec == null) {
            return null;
        }
        return new PdfExporter(exec);
    }

    private static File findExecutable() {
        String direct = System.getProperty(KEY_DOT);
        if (direct != null) {
            File file = new File(direct);
            if (file.exists() && file.canExecute()) {
                return file;
            }
        }
        String path = System.getenv("PATH"); //$NON-NLS-1$
        if (path == null) {
            return null;
        }
        for (String s : path.split(Pattern.quote(File.pathSeparator))) {
            if (s.trim().isEmpty()) {
                continue;
            }
            File base = new File(s);
            if (base.isDirectory() == false) {
                continue;
            }
            File exec = new File(base, "dot"); //$NON-NLS-1$
            if (exec.isFile() && exec.canExecute()) {
                return exec;
            }
        }
        return null;
    }

    /**
     * Exports the target inspection object as PDF file.
     * @param output the output target file
     * @param node the export target node
     * @param verbose {@code true} to output detail graph
     * @throws IOException if failed to export a file
     * @throws InterruptedException if interrupted while executing DOT
     */
    public void export(File output, InspectionNode node, boolean verbose) throws IOException, InterruptedException {
        File parent = output.getAbsoluteFile().getParentFile();
        if (parent.mkdirs() == false && parent.isDirectory() == false) {
            throw new IOException(MessageFormat.format(
                    "failed to create file: {0}",
                    output));
        }
        List<String> command = new ArrayList<>();
        command.add(dot.getAbsolutePath());
        command.add("-Tpdf"); //$NON-NLS-1$
        ProcessBuilder builder = new ProcessBuilder(command);
        builder.redirectOutput(output);
        builder.redirectError(Redirect.INHERIT);
        Process process = builder.start();
        try {
            try (OutputStream stdin = process.getOutputStream()) {
                DotProcessor.process(node, stdin, verbose);
            }
            int status = process.waitFor();
            if (status != 0) {
                throw new IOException(MessageFormat.format(
                        "DOT returns non-zero exit code: {0} (code={1})",
                        dot,
                        status));
            }
        } finally {
            process.destroy();
        }
    }
}
