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
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asakusafw.lang.info.BatchInfo;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.airlift.airline.Arguments;
import io.airlift.airline.Command;
import io.airlift.airline.Option;

/**
 * A command for printing list of batch.
 * @since 0.4.2
 */
@Command(
        name = "batch",
        description = "Displays list of batches",
        hidden = false
)
public class ListBatchCommand extends BaseCommand {

    static final Logger LOG = LoggerFactory.getLogger(ListBatchCommand.class);

    @Arguments(
            title = "batchapps",
            description = "batch applications directory",
            required = false,
            usage = "/path/to/batchapps")
    File batchApplicationDir = ListUtil.ASAKUSA_BATCHAPPS_HOME;

    @Option(
            name = { "--verbose", "-v", },
            title = "verbose mode",
            description = "verbose mode",
            arity = 0,
            required = false)
    boolean showVerbose = false;

    @Override
    protected Status process(PrintWriter writer) {
        if (batchApplicationDir == null) {
            LOG.error("batch applications directory is not specified");
            return Status.PREPARE_ERROR;
        }
        List<File> batchapps = Optional.ofNullable(batchApplicationDir.listFiles())
            .map(Arrays::asList)
            .orElse(Collections.emptyList())
            .stream()
            .filter(it -> ListUtil.findBatchInfo(it).isPresent())
            .sorted(Comparator.comparing(File::getName))
            .collect(Collectors.toList());
        if (showVerbose) {
            ObjectMapper mapper = new ObjectMapper();
            for (File batchapp : batchapps) {
                File infoFile = ListUtil.findBatchInfo(batchapp).get();
                try {
                    BatchInfo info = mapper.readValue(infoFile, BatchInfo.class);
                    Map<String, Object> members = new LinkedHashMap<>();
                    members.put("class", info.getDescriptionClass());
                    members.put("comment", info.getComment());
                    writer.printf("%s:%n", info.getId());
                    ListUtil.printBlock(writer, 4, members);
                } catch (IOException e) {
                    LOG.error("error occurred while loading batch information: {}", infoFile, e);
                }
            }
        } else {
            batchapps.forEach(it -> writer.println(it.getName()));
        }
        return Status.SUCCESS;
    }
}
