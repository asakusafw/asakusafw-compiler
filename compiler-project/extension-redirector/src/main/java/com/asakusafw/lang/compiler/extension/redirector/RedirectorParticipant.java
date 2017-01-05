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
package com.asakusafw.lang.compiler.extension.redirector;

import java.io.File;
import java.io.IOException;
import java.text.MessageFormat;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asakusafw.lang.compiler.api.BatchProcessor;
import com.asakusafw.lang.compiler.api.CompilerOptions;
import com.asakusafw.lang.compiler.api.reference.BatchReference;
import com.asakusafw.lang.compiler.api.reference.JobflowReference;
import com.asakusafw.lang.compiler.common.Diagnostic;
import com.asakusafw.lang.compiler.common.DiagnosticException;
import com.asakusafw.lang.compiler.common.Location;
import com.asakusafw.lang.compiler.core.BatchCompiler.Context;
import com.asakusafw.lang.compiler.core.basic.AbstractCompilerParticipant;
import com.asakusafw.lang.compiler.core.basic.JobflowPackager;
import com.asakusafw.lang.compiler.model.graph.Batch;
import com.asakusafw.lang.compiler.redirector.RedirectRule;
import com.asakusafw.lang.compiler.redirector.ZipRewriter;

/**
 * Redirects API accesses into another classes.
 * An implementation of {@link BatchProcessor} for redirecting API invocations.
 */
public class RedirectorParticipant extends AbstractCompilerParticipant {

    static final Logger LOG = LoggerFactory.getLogger(RedirectorParticipant.class);

    /**
     * The compiler option property key prefix of the redirector rules.
     */
    public static final String KEY_RULE_PREFIX = "redirector.rule."; //$NON-NLS-1$

    @Override
    public void afterBatch(Context context, Batch batch, BatchReference reference) {
        RedirectRule rule = extractRule(context.getOptions());
        if (rule.isEmpty()) {
            return;
        }
        ZipRewriter rewriter = new ZipRewriter(rule);
        LOG.debug("redirecting API invocations: {}", batch.getBatchId()); //$NON-NLS-1$
        for (JobflowReference jobflow : reference.getJobflows()) {
            Location location = JobflowPackager.getLibraryLocation(jobflow.getFlowId());
            File file = context.getOutput().toFile(location);
            if (file.isFile()) {
                try {
                    rewriter.rewrite(file);
                } catch (IOException e) {
                    throw new DiagnosticException(Diagnostic.Level.ERROR, MessageFormat.format(
                            "error occurred while rewriting jobflow JAR file: {0}",
                            jobflow.getFlowId()), e);
                }
            } else {
                LOG.warn(MessageFormat.format(
                        "jobflow library file is not found: {0}",
                        file));
            }
        }
    }

    private static RedirectRule extractRule(CompilerOptions options) {
        RedirectRule results = new RedirectRule();
        for (Map.Entry<String, String> entry : options.getProperties(KEY_RULE_PREFIX).entrySet()) {
            String from = entry.getKey().substring(KEY_RULE_PREFIX.length());
            String to = entry.getValue();
            results.add(from, to);
        }
        return results;
    }
}
