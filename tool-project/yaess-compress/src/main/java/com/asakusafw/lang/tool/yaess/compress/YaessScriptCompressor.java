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
package com.asakusafw.lang.tool.yaess.compress;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asakusafw.yaess.core.BatchScript;

/**
 * Compresses YAESS script.
 */
public class YaessScriptCompressor {

    static final Logger LOG = LoggerFactory.getLogger(YaessScriptCompressor.class);

    /**
     * The YAESS script path (relative from the batch application).
     */
    public static final String PATH_SCRIPT = "etc/yaess-script.properties"; //$NON-NLS-1$

    /**
     * The pattern of the path of jobflow library (relative from the batch application).
     */
    static final String PATTERN_JOBFLOW = "lib/jobflow-{0}.jar"; //$NON-NLS-1$

    /**
     * The path suffix of backup files.
     */
    static final String SUFFIX_BACKUP = ".bak"; //$NON-NLS-1$

    private final File batchapp;

    /**
     * Creates a new instance.
     * @param batchapp the batch application folder
     */
    public YaessScriptCompressor(File batchapp) {
        this.batchapp = batchapp;
    }

    /**
     * Performs compressor.
     * @return {@code true} only if compressed
     * @throws IOException if failed to rebuild jobflow
     */
    public boolean perform() throws IOException {
        File scriptFile = new File(batchapp, PATH_SCRIPT);

        LOG.info(MessageFormat.format(
                "loading YAESS script: {0}",
                scriptFile));
        BatchScript script = Util.load(scriptFile);

        ScriptCompressor compressor = new ScriptCompressor(script);
        Map<String, List<String>> entries = compressor.getEntries();
        if (entries.isEmpty()) {
            LOG.info(MessageFormat.format(
                    "target YAESS script is hard to compress",
                    scriptFile));
            return false;
        }

        try (Installer installer = new Installer()) {
            LOG.info(MessageFormat.format(
                    "rebuilding YAESS script: {0}",
                    scriptFile));
            File compress = Util.save(Util.newTemporaryFile(scriptFile), compressor.getCompressed());
            installer.register(compress, scriptFile, Util.append(scriptFile, SUFFIX_BACKUP));
            for (Map.Entry<String, List<String>> entry : entries.entrySet()) {
                File flow = new File(batchapp, MessageFormat.format(PATTERN_JOBFLOW, entry.getKey()));
                LOG.info(MessageFormat.format(
                        "rebuilding jobflow: {0} ({1} stages in main phase)",
                        entry.getKey(),
                        entry.getValue().size()));
                File inject = inject(flow, entry.getValue());
                installer.register(inject, flow, Util.append(flow, SUFFIX_BACKUP));
            }
            LOG.info(MessageFormat.format(
                    "installing compressed batchapp: {0}",
                    batchapp));
            installer.install();
        }
        return true;
    }

    private File inject(File flow, List<String> classes) throws IOException {
        if (flow.isFile() == false) {
            throw new FileNotFoundException(flow.getPath());
        }
        File temp = Util.newTemporaryFile(flow);
        ClientInjector.inject(flow, temp, classes);
        return temp;
    }

    /**
     * The program entry.
     * @param args array of the target batch application folders
     */
    public static void main(String... args) {
        int status = exec(args);
        if (status != 0) {
            System.exit(status);
        }
    }

    static int exec(String... args) {
        if (args.length == 0) {
            System.err.printf("Usage: java -jar <this.jar> /path/to/batchapp [/path/to/batchapp [...]]");
            return 1;
        }
        List<File> targets = new ArrayList<>();
        for (String string : args) {
            File file = new File(string);
            if (new File(file, PATH_SCRIPT).isFile() == false) {
                LOG.error(MessageFormat.format(
                        "must be a valid Asakusa batch application: {0}",
                        file));
                return 1;
            }
            if (new File(file, PATH_SCRIPT + SUFFIX_BACKUP).isFile()) {
                LOG.info(MessageFormat.format(
                        "already applied: {0}",
                        file));
                continue;
            }
            targets.add(file);
        }
        if (targets.isEmpty()) {
            LOG.warn("there are no available batch applications");
            return 1;
        }
        for (File file : targets) {
            try {
                YaessScriptCompressor compressor = new YaessScriptCompressor(file);
                compressor.perform();
            } catch (IOException e) {
                LOG.error(MessageFormat.format(
                        "error occurred while processing application: {0}",
                        file), e);
                return 1;
            }
        }
        return 0;
    }
}
