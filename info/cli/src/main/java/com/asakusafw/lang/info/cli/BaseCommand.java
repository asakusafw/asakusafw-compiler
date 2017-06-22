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

import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.Charset;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.airlift.airline.Option;

/**
 * An abstract super implementation of commands.
 * @since 0.4.2
 */
public abstract class BaseCommand implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(BaseCommand.class);

    @Option(
            name = { "--output", "-o" },
            title = "file",
            description = "output file",
            arity = 1,
            required = false)
    File output;

    @Option(
            name = { "--encoding", "-e" },
            title = "encoding",
            description = "output character encoding",
            arity = 1,
            required = false)
    String encoding = Charset.defaultCharset().name();

    private Status status = Status.INITIALIZED;

    @Override
    public final void run() {
        this.status = process0();
    }

    private Status process0() {
        try {
            if (output != null) {
                File dir = output.getParentFile();
                if (dir != null) {
                    if (dir.mkdirs() == false && dir.isDirectory() == false) {
                        LOG.error("error occurred while creating output file: {}", output);
                        return Status.PREPARE_ERROR;
                    }
                }
                try (PrintWriter writer = new PrintWriter(output, encoding)) {
                    return process(writer);
                }
            } else {
                PrintWriter writer = new PrintWriter(new OutputStreamWriter(System.out, encoding), true);
                Status s = process(writer);
                writer.flush();
                return s;
            }
        } catch (IOException e) {

            LOG.error("error occurred while processing command", e);
            return Status.PROCESS_ERROR;
        }
    }

    /**
     * Returns the command status.
     * @return the command status
     */
    protected Status getStatus() {
        return status;
    }

    /**
     * Executes the command body.
     * @param writer the output writer
     * @return the command body
     */
    protected abstract Status process(PrintWriter writer);

    /**
     * Represents command status.
     * @since 0.4.2
     */
    protected enum Status {

        /**
         * Initialized.
         */
        INITIALIZED,

        /**
         * Completed without errors.
         */
        SUCCESS,

        /**
         * Failed at preparing command.
         */
        PREPARE_ERROR,

        /**
         * Failed at processing command.
         */
        PROCESS_ERROR,
    }
}
