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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.text.MessageFormat;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asakusafw.yaess.core.BatchScript;
import com.asakusafw.yaess.core.FlowScript;

final class Util {

    static final Logger LOG = LoggerFactory.getLogger(Util.class);

    private Util() {
        return;
    }

    static String getClientClassName() {
        String name = Util.class.getName();
        int index = name.lastIndexOf('.');
        assert index >= 0;
        return name.substring(0, index) + ".Client"; //$NON-NLS-1$
    }

    static BatchScript load(File file) throws IOException {
        Properties properties = new Properties();
        try (InputStream in = new FileInputStream(file)) {
            properties.load(in);
        }
        return BatchScript.load(properties);
    }

    static File save(File file, BatchScript source) throws IOException {
        Properties properties = new Properties();
        properties.setProperty(BatchScript.KEY_ID, source.getId());
        properties.setProperty(BatchScript.KEY_VERSION, BatchScript.VERSION);
        properties.setProperty(BatchScript.KEY_VERIFICATION_CODE, source.getBuildId());
        for (FlowScript script : source.getAllFlows()) {
            script.storeTo(properties);
        }
        try (OutputStream out = create(file)) {
            properties.store(out, null);
        }
        return file;
    }

    static void copy(InputStream input, OutputStream output) throws IOException {
        byte[] buf = new byte[256];
        while (true) {
            int read = input.read(buf);
            if (read < 0) {
                break;
            }
            output.write(buf, 0, read);
        }
    }

    static void delete(File file) throws IOException {
        if (file.delete() == false && file.exists()) {
            throw new IOException(MessageFormat.format(
                    "failed to delete file: {0}",
                    file));
        }
    }

    static void deleteSoft(File file) {
        if (file.delete() == false && file.exists()) {
            LOG.warn(MessageFormat.format(
                    "failed to delete a temporary file: {0}",
                    file));
        }
    }

    static OutputStream create(File file) throws IOException {
        prepare(file);
        return new FileOutputStream(file);
    }

    static void prepare(File file) throws IOException {
        File parent = file.getAbsoluteFile().getParentFile();
        if (parent.mkdirs() == false && parent.isDirectory() == false) {
            throw new IOException(MessageFormat.format(
                    "failed to create file: {0}",
                    file));
        }
    }

    static File newTemporaryFile(File hint) throws IOException {
        String name = hint.getName();
        String suffix = ".tmp"; //$NON-NLS-1$
        int index = name.lastIndexOf('.');
        if (index >= 0) {
            suffix = name.substring(index);
        }
        return File.createTempFile("asakusa", suffix); //$NON-NLS-1$
    }

    static File append(File file, String suffix) {
        return new File(file.getPath() + suffix);
    }
}
