/*
 * Key Value Mapping for Microsoft Azure EventHub
 * Copyright (C) 2024 Suomen Kanuuna Oy
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 *
 *
 * Additional permission under GNU Affero General Public License version 3
 * section 7
 *
 * If you modify this Program, or any covered work, by linking or combining it
 * with other code, such other code is not for that reason alone subject to any
 * of the requirements of the GNU Affero GPL version 3 as long as this Program
 * is the same Program as licensed from Suomen Kanuuna Oy without any additional
 * modifications.
 *
 * Supplemented terms under GNU Affero General Public License version 3
 * section 7
 *
 * Origin of the software must be attributed to Suomen Kanuuna Oy. Any modified
 * versions must be marked as "Modified version of" The Program.
 *
 * Names of the licensors and authors may not be used for publicity purposes.
 *
 * No rights are granted for use of trade names, trademarks, or service marks
 * which are in The Program if any.
 *
 * Licensee must indemnify licensors and authors for any liability that these
 * contractual assumptions impose on licensors and authors.
 *
 * To the extent this program is licensed as part of the Commercial versions of
 * Teragrep, the applicable Commercial License may apply to this file if you as
 * a licensee so wish it.
 */
package com.teragrep.akv_01.plugin;

import jakarta.json.JsonValue;
import jakarta.json.JsonException;
import jakarta.json.JsonObject;
import jakarta.json.JsonStructure;
import jakarta.json.JsonString;
import jakarta.json.JsonArray;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public final class PluginMap {

    private final JsonStructure json;

    public PluginMap(final JsonStructure json) {
        this.json = json;
    }

    private void assertType(final String msg, final JsonValue value, final JsonValue.ValueType type)
            throws IllegalArgumentException {
        if (value == null) {
            throw new JsonException("Expected <" + type + "> but got no value " + msg);
        }

        if (!value.getValueType().equals(type)) {
            throw new JsonException("Expected <" + type + "> but got <" + value.getValueType() + "> " + msg);
        }
    }

    private void assertType(final JsonStructure parentStructure, final String key, final JsonValue.ValueType type) {
        final JsonValue value = parentStructure.getValue("/" + key);
        assertType(key, value, type);
    }

    public Map<String, String> asMap() {
        final Map<String, String> map = new HashMap<>();

        assertType("in top-level structure", json, JsonValue.ValueType.OBJECT);
        final JsonObject mainObject = json.asJsonObject();

        if (mainObject.isEmpty()) {
            throw new JsonException("Expected top-level structure to be a non-empty object");
        }

        assertType(mainObject, "defaultPluginClass", JsonValue.ValueType.STRING);

        assertType(mainObject, "resourceIds", JsonValue.ValueType.ARRAY);
        final JsonArray resourceIdPlugins = mainObject.getJsonArray("resourceIds");

        for (final JsonValue jsonValue : resourceIdPlugins) {
            assertType("in resourceIds array item", jsonValue, JsonValue.ValueType.OBJECT);
            final JsonObject pluginObject = jsonValue.asJsonObject();

            assertType(pluginObject, "resourceId", JsonValue.ValueType.STRING);
            final JsonString id = pluginObject.getJsonString("resourceId");

            assertType(pluginObject, "pluginClass", JsonValue.ValueType.STRING);
            final JsonString className = pluginObject.getJsonString("pluginClass");

            if (id.getString().isEmpty()) {
                throw new JsonException("ResourceId is empty");
            }

            if (className.getString().isEmpty()) {
                throw new JsonException("PluginClass is empty");
            }

            if (map.containsKey(id.getString())) {
                throw new JsonException("Duplicate plugin id " + id);
            }

            map.put(id.getString(), className.getString());
        }

        return map;
    }

    @Override
    public boolean equals(final Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final PluginMap pluginMap = (PluginMap) o;
        return Objects.equals(json, pluginMap.json);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(json);
    }
}
