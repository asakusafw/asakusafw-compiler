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
package com.asakusafw.dag.compiler.extension.directio.testing;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import com.asakusafw.lang.compiler.common.testing.FileEditor;
import com.asakusafw.lang.compiler.mapreduce.testing.mock.DirectIoContext;
import com.asakusafw.lang.compiler.mapreduce.testing.mock.WritableDataFormat;
import com.asakusafw.lang.utils.common.Action;
import com.asakusafw.lang.utils.common.Invariants;
import com.asakusafw.lang.utils.common.Optionals;
import com.asakusafw.runtime.io.ModelInput;
import com.asakusafw.runtime.io.ModelOutput;

/**
 * Helper for testing with Direct I/O.
 */
public class DirectIoTestHelper implements TestRule {

    private DirectIoContext context;

    @Override
    public Statement apply(Statement base, Description description) {
        context = new DirectIoContext();
        return context.apply(base, description);
    }

    /**
     * Returns the current Direct I/O context.
     * @return the current Direct I/O context
     */
    public DirectIoContext getContext() {
        Invariants.requireNonNull(context);
        return context;
    }

    /**
     * Adds an input file onto the current context.
     * @param <T> the data type
     * @param formatClass the format class
     * @param filePath the target Direct I/O file path
     * @param action the contents action
     */
    public <T extends Writable> void input(
            String filePath,
            Class<? extends WritableDataFormat<T>> formatClass,
            Action<ModelOutput<T>, ?> action) {
        Invariants.requireNonNull(context);
        File file = context.file(filePath);
        WritableDataFormat<T> format = ReflectionUtils.newInstance(formatClass, context.newConfiguration());
        Class<T> dataType = format.getSupportedType();
        try (ModelOutput<T> output = format.createOutput(dataType, filePath, FileEditor.create(file))) {
            action.perform(output);
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    /**
     * Adds an input file onto the current context.
     * @param <T> the data type
     * @param directoryPath the source Direct I/O file path
     * @param filePattern the file pattern
     * @param formatClass the format class
     * @param action the contents action
     */
    public <T extends Writable> void output(
            String directoryPath,
            String filePattern,
            Class<? extends WritableDataFormat<T>> formatClass,
            Action<List<T>, ?> action) {
        Invariants.requireNonNull(context);
        File directory = context.file(directoryPath);
        Predicate<String> filter = toFileNamePredicate(filePattern);
        List<File> files = Optionals.of(directory.listFiles())
            .map(list -> Stream.of(list)
                    .filter(f -> filter.test(f.getName()))
                    .collect(Collectors.toList()))
            .orElseGet(Collections::emptyList);
        WritableDataFormat<T> format = ReflectionUtils.newInstance(formatClass, context.newConfiguration());
        Class<T> dataType = format.getSupportedType();
        List<T> results = new ArrayList<>();
        try {
            for (File f: files) {
                long length = f.length();
                try (ModelInput<T> input = format.createInput(dataType, f.getPath(), FileEditor.open(f), 0, length)) {
                    while (true) {
                        T copy = dataType.newInstance();
                        if (input.readTo(copy) == false) {
                            break;
                        }
                        results.add(copy);
                    }
                }
            }
            action.perform(results);
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    private static Predicate<String> toFileNamePredicate(String filePattern) {
        int wildcard = filePattern.indexOf('*');
        if (wildcard < 0) {
            return s -> s.equals(filePattern);
        } else {
            String prefix = filePattern.substring(0, wildcard);
            String suffix = filePattern.substring(wildcard + 1);
            return s -> s.length() >= filePattern.length() - 1
                    && s.startsWith(prefix)
                    && s.endsWith(suffix);
        }
    }
}
