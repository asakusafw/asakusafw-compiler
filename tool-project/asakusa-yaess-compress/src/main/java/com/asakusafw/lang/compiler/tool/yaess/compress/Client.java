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

import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Tool;

/**
 * A Hadoop client that invokes the original clients.
 */
public class Client extends Configured implements Tool {

    /**
     * The path of original client class names.
     */
    public static final String PATH_ENTRIES = "META-INF/asakusa-yaess/entries";

    /**
     * The encoding of {@link #PATH_ENTRIES}.
     */
    public static final String ENCODING = "UTF-8";

    @Override
    public int run(String[] args) throws Exception {
        ClassLoader loader = getConf().getClassLoader();
        URL resource = loader.getResource(PATH_ENTRIES);
        if (resource == null) {
            throw new FileNotFoundException(PATH_ENTRIES);
        }
        List<Tool> entries = new ArrayList<>();
        try (Scanner scanner = new Scanner(new InputStreamReader(resource.openStream(), ENCODING))) {
            while (scanner.hasNextLine()) {
                String line = scanner.nextLine().trim();
                if (line.isEmpty()) {
                    continue;
                }
                Class<? extends Tool> aClass = getConf().getClassByName(line).asSubclass(Tool.class);
                Configuration copy = copy(getConf());
                Tool tool = ReflectionUtils.newInstance(aClass, copy);
                entries.add(tool);
            }
        }
        for (Tool entry : entries) {
            int status = entry.run(args.clone());
            if (status != 0) {
                return status;
            }
        }
        return 0;
    }

    private Configuration copy(Configuration conf) {
        Configuration copy = new Configuration(conf);
        copy.setClassLoader(conf.getClassLoader());
        return copy;
    }
}
