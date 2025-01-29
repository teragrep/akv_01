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
import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.*;

public final class ParsedEventListFactoryTest {

    @Test
    void testAsListMethod() {
        final String payload = Json.createObjectBuilder().add("resourceId", "123").build().toString();
        final String[] payloads = new String[] {
                payload, "string payload", payload
        };
        final Map<String, Object> partitionCtx = new HashMap<>();
        final Map<String, Object>[] propArray = new Map[] {
                new HashMap<String, Object>(), new HashMap<String, Object>(), new HashMap<String, Object>()
        };
        final Map<String, Object>[] sysPropArray = new Map[] {
                new HashMap<String, Object>(), new HashMap<String, Object>(), new HashMap<String, Object>()
        };
        final List<Object> enqueuedTimeList = Arrays
                .asList("2010-01-01T00:00:00", "2020-01-01T01:02:03", "2030-04-07T12:34:10");
        final List<String> offsetList = Arrays.asList("0", "1", "2");

        final ParsedEventListFactory arrayFactory = new ParsedEventListFactory(
                payloads,
                partitionCtx,
                propArray,
                sysPropArray,
                enqueuedTimeList,
                offsetList
        );

        final List<ParsedEvent> events = Assertions.assertDoesNotThrow(arrayFactory::asList);
        Assertions.assertEquals(3, events.size());
        Assertions.assertEquals(JSONEvent.class, events.get(0).getClass());
        Assertions.assertEquals(PlainEvent.class, events.get(1).getClass());
        Assertions.assertEquals(JSONEvent.class, events.get(2).getClass());
        Assertions.assertEquals(ZonedDateTime.of(2010, 1, 1, 0, 0, 0, 0, ZoneId.of("Z")), events.get(0).enqueuedTime());
        Assertions.assertEquals(ZonedDateTime.of(2020, 1, 1, 1, 2, 3, 0, ZoneId.of("Z")), events.get(1).enqueuedTime());
        Assertions
                .assertEquals(ZonedDateTime.of(2030, 4, 7, 12, 34, 10, 0, ZoneId.of("Z")), events.get(2).enqueuedTime());
        Assertions.assertEquals("123", events.get(0).resourceId());
        Assertions.assertThrows(UnsupportedOperationException.class, events.get(1)::resourceId);
        Assertions.assertEquals("123", events.get(2).resourceId());
    }

    @Test
    void testEqualsContract() {
        EqualsVerifier.forClass(ParsedEventListFactory.class).verify();
    }
}
