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
package com.asakusafw.lang.compiler.inspection.json;

import java.lang.reflect.Type;

import com.asakusafw.lang.compiler.inspection.InspectionNode;
import com.asakusafw.lang.compiler.inspection.InspectionNode.PortReference;
import com.google.gson.JsonArray;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;

/**
 * JSON adapter for port reference of {@link InspectionNode}.
 */
public class PortReferenceAdapter implements JsonAdapter<InspectionNode.PortReference> {

    @Override
    public InspectionNode.PortReference deserialize(
            JsonElement json,
            Type typeOfT,
            JsonDeserializationContext context) throws JsonParseException {
        if (json.isJsonArray() == false) {
            throw new JsonParseException("port reference must be an array"); //$NON-NLS-1$
        }
        JsonArray array = (JsonArray) json;
        if (array.size() != 2) {
            throw new JsonParseException("port reference must have just 2 elements"); //$NON-NLS-1$
        }
        String nodeId = array.get(0).getAsString();
        String portId = array.get(1).getAsString();
        return new PortReference(nodeId, portId);
    }

    @Override
    public JsonElement serialize(
            InspectionNode.PortReference src,
            Type typeOfSrc,
            JsonSerializationContext context) {
        JsonArray result = new JsonArray();
        result.add(new JsonPrimitive(src.getNodeId()));
        result.add(new JsonPrimitive(src.getPortId()));
        return result;
    }
}
