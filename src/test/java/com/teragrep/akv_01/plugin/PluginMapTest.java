/*
 * Teragrep Key Value Mapping for Microsoft Azure EventHub
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

import jakarta.json.Json;
import jakarta.json.JsonException;
import jakarta.json.JsonStructure;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

public class PluginMapTest {

    @Test
    void testEmptyJsonObject() {
        JsonStructure jsonStructure = Json.createObjectBuilder().build();
        PluginMap pluginMap = new PluginMap(jsonStructure);
        JsonException je = Assertions.assertThrows(JsonException.class, pluginMap::asUnmodifiableMap);
        Assertions.assertEquals("Expected top-level structure to be a non-empty object", je.getMessage());
    }

    @Test
    void testEmptyJsonArray() {
        JsonStructure jsonStructure = Json.createArrayBuilder().build();
        PluginMap pluginMap = new PluginMap(jsonStructure);
        JsonException je = Assertions.assertThrows(JsonException.class, pluginMap::asUnmodifiableMap);
        Assertions.assertEquals("Expected <OBJECT> but got <[ARRAY]> in top-level structure", je.getMessage());
    }

    @Test
    void testNoDefaultPluginFactoryClassName() {
        JsonStructure jsonStructure = Json.createObjectBuilder().add("resourceIds", Json.createArrayBuilder()).build();
        PluginMap pluginMap = new PluginMap(jsonStructure);
        JsonException je = Assertions.assertThrows(JsonException.class, pluginMap::asUnmodifiableMap);
        Assertions
                .assertEquals(
                        "Non-existing name/value pair in the object for key defaultPluginFactoryClass", je.getMessage()
                );
    }

    @Test
    void testCorrectJson() {
        JsonStructure jsonStructure = Json
                .createObjectBuilder()
                .add("defaultPluginFactoryClass", "com.teragrep.akv_01.PluginImpl")
                .add("exceptionPluginFactoryClass", "com.teragrep.akv_01.ExceptionPlugin")
                .add("resourceIds", Json.createArrayBuilder().add(Json.createObjectBuilder().add("resourceId", "id").add("pluginFactoryClass", "class").add("pluginFactoryConfig", "/src/test/resources/class.json"))).build();
        PluginMap pluginMap = new PluginMap(jsonStructure);
        final Map<String, PluginFactoryConfig> map = Assertions.assertDoesNotThrow(pluginMap::asUnmodifiableMap);
        Assertions.assertEquals("class", map.get("id").pluginFactoryClassName());
        Assertions.assertEquals("/src/test/resources/class.json", map.get("id").configPath());
        Assertions.assertEquals("com.teragrep.akv_01.PluginImpl", pluginMap.defaultPluginFactoryClassName());
        Assertions.assertEquals("com.teragrep.akv_01.ExceptionPlugin", pluginMap.exceptionPluginFactoryClassName());
    }

    @Test
    void testEqualsContract() {
        EqualsVerifier.simple().forClass(PluginMap.class).verify();
    }
}
