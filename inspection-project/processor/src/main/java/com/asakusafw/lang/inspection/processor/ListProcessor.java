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
package com.asakusafw.lang.inspection.processor;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import com.asakusafw.lang.inspection.InspectionNode;

/**
 * Prints list of elements in UTF-8 string.
 */
public class ListProcessor implements InspectionNodeProcessor {

    static final Charset ENCODING = StandardCharsets.UTF_8;

    @Override
    public void process(Context context, InspectionNode node, OutputStream output) throws IOException {
        PrintWriter writer = new PrintWriter(new OutputStreamWriter(output, ENCODING));
        for (String elementId : node.getElements().keySet()) {
            writer.println(elementId);
        }
        writer.flush();
    }
}
