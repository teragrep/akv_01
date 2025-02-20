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
package com.teragrep.akv_01.event;

import com.teragrep.akv_01.event.metadata.offset.EventOffset;
import com.teragrep.akv_01.event.metadata.offset.EventOffsetImpl;
import com.teragrep.akv_01.event.metadata.partitionContext.EventPartitionContext;
import com.teragrep.akv_01.event.metadata.partitionContext.EventPartitionContextImpl;
import com.teragrep.akv_01.event.metadata.properties.EventProperties;
import com.teragrep.akv_01.event.metadata.properties.EventPropertiesImpl;
import com.teragrep.akv_01.event.metadata.systemProperties.EventSystemProperties;
import com.teragrep.akv_01.event.metadata.systemProperties.EventSystemPropertiesImpl;
import com.teragrep.akv_01.event.metadata.time.EnqueuedTime;
import com.teragrep.akv_01.event.metadata.time.EnqueuedTimeImpl;
import jakarta.json.Json;
import jakarta.json.JsonStructure;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;

public final class MultiRecordEventTest {

    @Test
    void testJsonPayloadWithRecords() {
        final String payload = Json
                .createObjectBuilder()
                .add("records", Json.createArrayBuilder().add(Json.createObjectBuilder().add("key", "value").build()).add(Json.createObjectBuilder().add("key", "value").build())).build().toString();
        final EventPartitionContext partitionContext = new EventPartitionContextImpl(new HashMap<>());
        final EventProperties properties = new EventPropertiesImpl(new HashMap<>());
        final EventSystemProperties systemProperties = new EventSystemPropertiesImpl(new HashMap<>());
        final EnqueuedTime enqueuedTimeUtc = new EnqueuedTimeImpl("2010-01-01T00:00:00");
        final EventOffset offset = new EventOffsetImpl("0");
        UnparsedEvent impl = new UnparsedEventImpl(
                payload,
                partitionContext,
                properties,
                systemProperties,
                enqueuedTimeUtc,
                offset
        );

        ParsedEvent parsed = new ParsedEventFactory(impl).parsedEvent();
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
        Assertions.assertEquals(parsed.enqueuedTimeUtc(), r1.enqueuedTimeUtc());
        Assertions.assertEquals(parsed.offset(), r1.offset());
        Assertions.assertEquals(parsed.partitionCtx(), r1.partitionCtx());
        Assertions.assertEquals(parsed.properties(), r1.properties());
        Assertions.assertEquals(parsed.systemProperties(), r1.systemProperties());

        Assertions.assertTrue(r2.isJsonStructure());
        JsonStructure jsonStructure2 = Assertions.assertDoesNotThrow(r2::asJsonStructure);
        Assertions.assertEquals("value", jsonStructure2.asJsonObject().getString("key"));
        Assertions.assertEquals(parsed.enqueuedTimeUtc(), r2.enqueuedTimeUtc());
        Assertions.assertEquals(parsed.offset(), r2.offset());
        Assertions.assertEquals(parsed.partitionCtx(), r2.partitionCtx());
        Assertions.assertEquals(parsed.properties(), r2.properties());
        Assertions.assertEquals(parsed.systemProperties(), r2.systemProperties());
    }

    @Test
    void testJsonPayloadWithStringRecords() {
        final String payload = Json
                .createObjectBuilder()
                .add("records", Json.createArrayBuilder().add("string1").add("string2").add("string3"))
                .build()
                .toString();

        final EventPartitionContext partitionContext = new EventPartitionContextImpl(new HashMap<>());
        final EventProperties properties = new EventPropertiesImpl(new HashMap<>());
        final EventSystemProperties systemProperties = new EventSystemPropertiesImpl(new HashMap<>());
        final EnqueuedTime enqueuedTimeUtc = new EnqueuedTimeImpl("2010-01-01T00:00:00");
        final EventOffset offset = new EventOffsetImpl("0");
        UnparsedEvent impl = new UnparsedEventImpl(
                payload,
                partitionContext,
                properties,
                systemProperties,
                enqueuedTimeUtc,
                offset
        );

        ParsedEvent parsed = new ParsedEventFactory(impl).parsedEvent();
        Assertions.assertTrue(parsed.isJsonStructure());
        JsonStructure jsonStructure = Assertions.assertDoesNotThrow(parsed::asJsonStructure);
        Assertions.assertTrue(jsonStructure.asJsonObject().containsKey("records"));

        MultiRecordEvent mre = new MultiRecordEvent(parsed);
        Assertions.assertFalse(mre.isValid());
        Assertions.assertThrows(IllegalStateException.class, mre::records);
    }

    @Test
    void testWithJsonPayload() {
        final String payload = Json.createObjectBuilder().build().toString();
        final EventPartitionContext partitionContext = new EventPartitionContextImpl(new HashMap<>());
        final EventProperties properties = new EventPropertiesImpl(new HashMap<>());
        final EventSystemProperties systemProperties = new EventSystemPropertiesImpl(new HashMap<>());
        final EnqueuedTime enqueuedTimeUtc = new EnqueuedTimeImpl("2010-01-01T00:00:00");
        final EventOffset offset = new EventOffsetImpl("0");
        UnparsedEvent impl = new UnparsedEventImpl(
                payload,
                partitionContext,
                properties,
                systemProperties,
                enqueuedTimeUtc,
                offset
        );

        ParsedEvent parsed = new ParsedEventFactory(impl).parsedEvent();
        Assertions.assertTrue(parsed.isJsonStructure());
        Assertions.assertDoesNotThrow(parsed::asJsonStructure);

        MultiRecordEvent mre = new MultiRecordEvent(parsed);
        Assertions.assertFalse(mre.isValid());
        Assertions.assertThrows(IllegalStateException.class, mre::records);
    }

    @Test
    void testWithPlainPayload() {
        final String payload = "abc";
        final EventPartitionContext partitionContext = new EventPartitionContextImpl(new HashMap<>());
        final EventProperties properties = new EventPropertiesImpl(new HashMap<>());
        final EventSystemProperties systemProperties = new EventSystemPropertiesImpl(new HashMap<>());
        final EnqueuedTime enqueuedTimeUtc = new EnqueuedTimeImpl("2010-01-01T00:00:00");
        final EventOffset offset = new EventOffsetImpl("0");
        UnparsedEvent impl = new UnparsedEventImpl(
                payload,
                partitionContext,
                properties,
                systemProperties,
                enqueuedTimeUtc,
                offset
        );

        ParsedEvent parsed = new ParsedEventFactory(impl).parsedEvent();
        Assertions.assertFalse(parsed.isJsonStructure());
        Assertions.assertThrows(UnsupportedOperationException.class, parsed::asJsonStructure);

        MultiRecordEvent mre = new MultiRecordEvent(parsed);
        Assertions.assertFalse(mre.isValid());
        Assertions.assertThrows(IllegalStateException.class, mre::records);
    }
}
