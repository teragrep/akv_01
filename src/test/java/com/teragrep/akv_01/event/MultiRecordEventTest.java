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
package com.teragrep.akv_01.event;

import jakarta.json.Json;
import jakarta.json.JsonStructure;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class MultiRecordEventTest {

    @Test
    void testJsonPayloadWithRecords() {
        final String payload = Json
                .createObjectBuilder()
                .add("records", Json.createArrayBuilder().add(Json.createObjectBuilder().add("key", "value").build()).add(Json.createObjectBuilder().add("key", "value").build())).build().toString();
        final Map<String, Object> partitionContext = new HashMap<>();
        final Map<String, Object> properties = new HashMap<>();
        final Map<String, Object> systemProperties = new HashMap<>();
        final Object enqueuedTimeUtc = "2010-01-01T00:00:00";
        final String offset = "0";
        Event impl = new EventImpl(payload, partitionContext, properties, systemProperties, enqueuedTimeUtc, offset);

        ParsedEvent parsed = impl.parsedEvent();
        Assertions.assertTrue(parsed.isJsonStructure());
        JsonStructure jsonStructure = Assertions.assertDoesNotThrow(parsed::asJsonStructure);
        Assertions.assertTrue(jsonStructure.asJsonObject().containsKey("records"));

        MultiRecordEvent mre = new MultiRecordEvent(parsed);
        Assertions.assertTrue(mre.isValid());
        List<ParsedEvent> records = Assertions.assertDoesNotThrow(mre::records);
        Assertions.assertEquals(2, records.size());

        ParsedEvent r1 = records.get(0);
        ParsedEvent r2 = records.get(1);

        Assertions.assertTrue(r1.isJsonStructure());
        JsonStructure jsonStructure1 = Assertions.assertDoesNotThrow(r1::asJsonStructure);
        Assertions.assertEquals("value", jsonStructure1.asJsonObject().getString("key"));
        Assertions.assertEquals(parsed.enqueuedTime(), r1.enqueuedTime());
        Assertions.assertEquals(parsed.offset(), r1.offset());
        Assertions.assertEquals(parsed.partitionContext(), r1.partitionContext());
        Assertions.assertEquals(parsed.properties(), r1.properties());
        Assertions.assertEquals(parsed.systemProperties(), r1.systemProperties());

        Assertions.assertTrue(r2.isJsonStructure());
        JsonStructure jsonStructure2 = Assertions.assertDoesNotThrow(r2::asJsonStructure);
        Assertions.assertEquals("value", jsonStructure2.asJsonObject().getString("key"));
        Assertions.assertEquals(parsed.enqueuedTime(), r2.enqueuedTime());
        Assertions.assertEquals(parsed.offset(), r2.offset());
        Assertions.assertEquals(parsed.partitionContext(), r2.partitionContext());
        Assertions.assertEquals(parsed.properties(), r2.properties());
        Assertions.assertEquals(parsed.systemProperties(), r2.systemProperties());
    }

    @Test
    void testWithJsonPayload() {
        final String payload = Json.createObjectBuilder().build().toString();
        final Map<String, Object> partitionContext = new HashMap<>();
        final Map<String, Object> properties = new HashMap<>();
        final Map<String, Object> systemProperties = new HashMap<>();
        final Object enqueuedTimeUtc = "2010-01-01T00:00:00";
        final String offset = "0";
        Event impl = new EventImpl(payload, partitionContext, properties, systemProperties, enqueuedTimeUtc, offset);

        ParsedEvent parsed = impl.parsedEvent();
        Assertions.assertTrue(parsed.isJsonStructure());
        Assertions.assertDoesNotThrow(parsed::asJsonStructure);

        MultiRecordEvent mre = new MultiRecordEvent(parsed);
        Assertions.assertFalse(mre.isValid());
        Assertions.assertThrows(IllegalStateException.class, mre::records);
    }

    @Test
    void testWithPlainPayload() {
        final String payload = "abc";
        final Map<String, Object> partitionContext = new HashMap<>();
        final Map<String, Object> properties = new HashMap<>();
        final Map<String, Object> systemProperties = new HashMap<>();
        final Object enqueuedTimeUtc = "2010-01-01T00:00:00";
        final String offset = "0";
        Event impl = new EventImpl(payload, partitionContext, properties, systemProperties, enqueuedTimeUtc, offset);

        ParsedEvent parsed = impl.parsedEvent();
        Assertions.assertFalse(parsed.isJsonStructure());
        Assertions.assertThrows(UnsupportedOperationException.class, parsed::asJsonStructure);

        MultiRecordEvent mre = new MultiRecordEvent(parsed);
        Assertions.assertFalse(mre.isValid());
        Assertions.assertThrows(IllegalStateException.class, mre::records);
    }
}
