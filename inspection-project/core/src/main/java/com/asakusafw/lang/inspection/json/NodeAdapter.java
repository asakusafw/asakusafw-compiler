/**
 * Copyright 2011-2018 Asakusa Framework Team.
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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import com.asakusafw.lang.inspection.InspectionNode;
import com.asakusafw.lang.inspection.InspectionNode.Port;
import com.asakusafw.lang.inspection.WithId;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.reflect.TypeToken;

/**
 * JSON adapter for {@link InspectionNode}.
 */
public class NodeAdapter implements JsonAdapter<InspectionNode> {

    private static final String KEY_ID = "id"; //$NON-NLS-1$

    private static final String KEY_TITLE = "title"; //$NON-NLS-1$

    private static final String KEY_INPUTS = "inputs"; //$NON-NLS-1$

    private static final String KEY_OUTPUTS = "outputs"; //$NON-NLS-1$

    private static final String KEY_PROPERTIES = "properties"; //$NON-NLS-1$

    private static final String KEY_ELEMENTS = "elements"; //$NON-NLS-1$

    private static final Type TYPE_PORTS = (new TypeToken<List<Port>>() {
        // empty
    }).getType();

    private static final Type TYPE_PROPERTIES = (new TypeToken<Map<String, String>>() {
        // empty
    }).getType();

    private static final Type TYPE_NODES = (new TypeToken<List<InspectionNode>>() {
        // empty
    }).getType();

    @Override
    public InspectionNode deserialize(
            JsonElement json,
            Type typeOfT,
            JsonDeserializationContext context) throws JsonParseException {
        if (json.isJsonObject() == false) {
            throw new JsonParseException("node must be an object"); //$NON-NLS-1$
        }
        JsonObject object = (JsonObject) json;
        String id = object.get(KEY_ID).getAsString();
        String title = object.get(KEY_TITLE).getAsString();
        List<Port> inputs = context.deserialize(object.get(KEY_INPUTS), TYPE_PORTS);
        List<Port> outputs = context.deserialize(object.get(KEY_OUTPUTS), TYPE_PORTS);
        Map<String, String> properties = context.deserialize(object.get(KEY_PROPERTIES), TYPE_PROPERTIES);
        List<InspectionNode> elements = context.deserialize(object.get(KEY_ELEMENTS), TYPE_NODES);

        InspectionNode result = new InspectionNode(id, title);
        put(inputs, result.getInputs());
        put(outputs, result.getOutputs());
        result.getProperties().putAll(properties);
        put(elements, result.getElements());
        return result;
    }

    @Override
    public JsonElement serialize(
            InspectionNode src,
            Type typeOfSrc,
            JsonSerializationContext context) {
        JsonObject result = new JsonObject();
        result.add(KEY_ID, new JsonPrimitive(src.getId()));
        result.add(KEY_TITLE, new JsonPrimitive(src.getTitle()));
        result.add(KEY_INPUTS, context.serialize(extract(src.getInputs()), TYPE_PORTS));
        result.add(KEY_OUTPUTS, context.serialize(extract(src.getOutputs()), TYPE_PORTS));
        result.add(KEY_PROPERTIES, context.serialize(src.getProperties(), TYPE_PROPERTIES));
        result.add(KEY_ELEMENTS, context.serialize(extract(src.getElements()), TYPE_NODES));
        return result;
    }

    private <T extends WithId> List<T> extract(Map<String, ? extends T> map) {
        return new ArrayList<>(map.values());
    }

    private <T extends WithId> void put(Collection<? extends T> elements, Map<String, ? super T> target) {
        for (T element : elements) {
            target.put(element.getId(), element);
        }
    }
}
