/**
 * Copyright 2011-2016 Asakusa Framework Team.
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
package com.asakusafw.lang.inspection.json;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;

import com.asakusafw.lang.inspection.InspectionNode;
import com.asakusafw.lang.inspection.InspectionNodeRepository;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonIOException;
import com.google.gson.JsonParseException;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;

/**
 * An implementation of {@link InspectionNodeRepository} using JSON format.
 */
public class JsonInspectionNodeRepository implements InspectionNodeRepository {

    private static final Charset ENCODING = Charset.forName("UTF-8"); //$NON-NLS-1$

    private final Gson gson = new GsonBuilder()
        .registerTypeAdapter(InspectionNode.class, new NodeAdapter())
        .registerTypeAdapter(InspectionNode.Port.class, new PortAdapter())
        .registerTypeAdapter(InspectionNode.PortReference.class, new PortReferenceAdapter())
        .create();

    @Override
    public InspectionNode load(InputStream input) throws IOException {
        try (JsonReader reader = new JsonReader(new InputStreamReader(input, ENCODING))) {
            InspectionNode result = gson.fromJson(reader, InspectionNode.class);
            if (result == null) {
                throw new IOException("there are no valid JSON object");
            }
            return result;
        } catch (JsonParseException e) {
            throw new IOException("invalid JSON object", e);
        }
    }

    @Override
    public void store(OutputStream output, InspectionNode node) throws IOException {
        try (JsonWriter writer = new JsonWriter(new OutputStreamWriter(output, ENCODING))) {
            gson.toJson(node, InspectionNode.class, writer);
        } catch (JsonIOException e) {
            throw new IOException("failed to store as JSON object", e);
        }
    }
}
