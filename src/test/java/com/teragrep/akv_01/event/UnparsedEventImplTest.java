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
import com.teragrep.akv_01.event.metadata.time.EnqueuedTimeStub;
import jakarta.json.JsonStructure;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;

public final class UnparsedEventImplTest {

    @Test
    void testWithNonJsonPayload() {
        final String payload = "payload here";
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
        Assertions.assertEquals("2010-01-01T00:00Z", parsed.enqueuedTimeUtc().zonedDateTime().toString());
    }

    @Test
    void testWithJsonPayload() {
        final String payload = "{\"resourceId\": \"12345\"}";
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
        Assertions.assertTrue(jsonStructure.asJsonObject().containsKey("resourceId"));
        Assertions.assertEquals("12345", jsonStructure.asJsonObject().getString("resourceId"));
        Assertions.assertEquals("12345", parsed.resourceId());
        Assertions.assertEquals("2010-01-01T00:00Z", parsed.enqueuedTimeUtc().zonedDateTime().toString());
    }

    @Test
    void testWithStubEnqueuedTime() {
        final String payload = "payload here";
        final EventPartitionContext partitionContext = new EventPartitionContextImpl(new HashMap<>());
        final EventProperties properties = new EventPropertiesImpl(new HashMap<>());
        final EventSystemProperties systemProperties = new EventSystemPropertiesImpl(new HashMap<>());
        final EnqueuedTime enqueuedTimeUtc = new EnqueuedTimeStub();
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
        Assertions.assertThrows(UnsupportedOperationException.class, () -> parsed.enqueuedTimeUtc().zonedDateTime());
    }

    @Test
    void testEqualsContract() {
        EqualsVerifier.forClass(UnparsedEventImpl.class).verify();
    }
}
