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

import java.lang.reflect.Type;
import java.util.Map;
import java.util.Set;

import com.asakusafw.lang.inspection.InspectionNode;
import com.asakusafw.lang.inspection.InspectionNode.PortReference;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.reflect.TypeToken;

/**
 * JSON adapter for port of {@link InspectionNode}.
 */
public class PortAdapter implements JsonAdapter<InspectionNode.Port> {

    private static final String KEY_ID = "id"; //$NON-NLS-1$

    private static final String KEY_PROPERTIES = "properties"; //$NON-NLS-1$

    private static final String KEY_OPPOSITES = "opposites"; //$NON-NLS-1$

    private static final Type TYPE_PROPERTIES = (new TypeToken<Map<String, String>>() {
        // empty
    }).getType();

    private static final Type TYPE_REFERENCES = (new TypeToken<Set<PortReference>>() {
        // empty
    }).getType();

    @Override
    public InspectionNode.Port deserialize(
            JsonElement json,
            Type typeOfT,
            JsonDeserializationContext context) throws JsonParseException {
        if (json.isJsonObject() == false) {
            throw new JsonParseException("port must be an object"); //$NON-NLS-1$
        }
        JsonObject object = (JsonObject) json;
        String id = object.get(KEY_ID).getAsString();
        Map<String, String> properties = context.deserialize(object.get(KEY_PROPERTIES), TYPE_PROPERTIES);
        Set<PortReference> opposites = context.deserialize(object.get(KEY_OPPOSITES), TYPE_REFERENCES);

        InspectionNode.Port result = new InspectionNode.Port(id);
        result.getProperties().putAll(properties);
        result.getOpposites().addAll(opposites);
        return result;
    }

    @Override
    public JsonElement serialize(
            InspectionNode.Port src,
            Type typeOfSrc,
            JsonSerializationContext context) {
        JsonObject result = new JsonObject();
        result.add(KEY_ID, new JsonPrimitive(src.getId()));
        result.add(KEY_PROPERTIES, context.serialize(src.getProperties(), TYPE_PROPERTIES));
        result.add(KEY_OPPOSITES, context.serialize(src.getOpposites(), TYPE_REFERENCES));
        return result;
    }
}
