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
import java.io.PrintWriter;
import java.text.MessageFormat;
import java.util.Objects;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asakusafw.lang.info.BatchInfo;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.airlift.airline.Arguments;

/**
 * An abstract implementation of information commands.
 * @since 0.4.2
 */
public abstract class InfoCommand extends BaseCommand {

    private static final Logger LOG = LoggerFactory.getLogger(InfoCommand.class);

    @Arguments(
            title = "batch ID",
            description = "batch ID, directory or information file",
            required = true,
            usage = "batch.id")
    File input;

    @Override
    protected final Status process(PrintWriter writer) {
        File infoFile = getInfoFile();
        LOG.debug("loading info file: {}", input);

        ObjectMapper mapper = new ObjectMapper();
        BatchInfo info;
        try {
            info = mapper.readValue(infoFile, BatchInfo.class);
        } catch (IOException e) {
            LOG.error("error occurred while loading batch information: {}", infoFile, e);
            return Status.PREPARE_ERROR;
        }
        try {
            process(writer, info);
            return Status.SUCCESS;
        } catch (IOException e) {
            LOG.error("error occurred while processing batch information: {}", infoFile, e);
            return Status.PROCESS_ERROR;
        }
    }

    private File getInfoFile() {
        if (input == null) {
            throw new IllegalStateException();
        }
        // just batch-info.json
        if (input.isFile()) {
            return input;
        }
        // may be a batch directory
        if (input.isDirectory()) {
            Optional<File> info = ListUtil.findBatchInfo(input);
            if (info.isPresent()) {
                return info.get();
            }
        }
        // may be a batch ID
        if (input.isAbsolute() == false
                && Objects.equals(input.getPath(), input.getName())
                && ListUtil.ASAKUSA_BATCHAPPS_HOME != null) {
            Optional<File> info = ListUtil.findBatchInfo(new File(ListUtil.ASAKUSA_BATCHAPPS_HOME, input.getName()));
            if (info.isPresent()) {
                return info.get();
            }
        }
        // not found
        throw new IllegalStateException(MessageFormat.format(
                "cannot find valid Asakusa batch application information: {0}",
                input));
    }

    /**
     * Processes the batch information.
     * @param writer the output printer
     * @param info the information
     * @throws IOException if I/O error was occurred while processing the file
     */
    protected abstract void process(PrintWriter writer, BatchInfo info) throws IOException;
}
