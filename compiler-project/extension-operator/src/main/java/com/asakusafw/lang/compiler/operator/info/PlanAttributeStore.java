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
package com.asakusafw.lang.compiler.operator.info;

import java.io.InputStream;
import java.io.OutputStream;
import java.text.MessageFormat;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asakusafw.lang.compiler.api.JobflowProcessor;
import com.asakusafw.lang.compiler.common.Location;
import com.asakusafw.lang.info.api.AttributeCollector;
import com.asakusafw.lang.info.plan.PlanAttribute;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Save/restore {@link PlanAttribute}.
 * @since 0.4.2
 */
public final class PlanAttributeStore {

    static final Logger LOG = LoggerFactory.getLogger(PlanAttributeCollector.class);

    static final Location SERIALIZE_LOCATION = Location.of("asakusafw-info/plan.json");

    private PlanAttributeStore() {
        return;
    }

    /**
     * Adds {@link PlanAttribute} into the current jobflow.
     * @param context the current processing context
     * @param attribute the target attribute
     */
    public static void save(JobflowProcessor.Context context, PlanAttribute attribute) {
        try (OutputStream output = context.addResourceFile(SERIALIZE_LOCATION)) {
            LOG.debug("saving execution plan info");
            ObjectMapper mapper = new ObjectMapper()
                    .setSerializationInclusion(Include.NON_NULL);
            mapper.writerFor(PlanAttribute.class).writeValue(output, attribute);
        } catch (Exception e) {
            LOG.warn(MessageFormat.format(
                    "error occurred while saving execution plan information: {0}",
                    SERIALIZE_LOCATION), e);
        }
    }

    /**
     * Restores {@link PlanAttribute} which previously saved.
     * @param context the current collecting context
     * @return the restored attribute, or {@code empty} if not saved
     */
    public static Optional<PlanAttribute> load(AttributeCollector.Context context) {
        try (InputStream input = context.findResourceFile(SERIALIZE_LOCATION)) {
            if (input != null) {
                LOG.debug("loading execution plan info");
                ObjectMapper mapper = new ObjectMapper();
                return Optional.ofNullable(mapper.readerFor(PlanAttribute.class).readValue(input));
            }
        } catch (Exception e) {
            LOG.warn(MessageFormat.format(
                    "error occurred while loading execution plan information: {0}",
                    SERIALIZE_LOCATION), e);
        }
        return Optional.empty();
    }

}
