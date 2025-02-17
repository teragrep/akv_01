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

import com.teragrep.akv_01.time.EnqueuedTime;
import com.teragrep.akv_01.time.EnqueuedTimeFactory;
import com.teragrep.akv_01.time.EnqueuedTimeStub;
import jakarta.json.Json;
import jakarta.json.JsonReader;
import jakarta.json.JsonStructure;
import jakarta.json.stream.JsonParsingException;

import java.io.StringReader;
import java.util.Map;
import java.util.Objects;

public final class EventImpl implements Event {

    private final String payload;
    private final Map<String, Object> partitionCtx;
    private final Map<String, Object> properties;
    private final Map<String, Object> systemProperties;
    private final EnqueuedTime enqueuedTimeUtc;
    private final String offset;

    public EventImpl(
            final String payload,
            final Map<String, Object> partitionCtx,
            final Map<String, Object> properties,
            final Map<String, Object> systemProperties,
            final Object enqueuedTimeUtc,
            final String offset
    ) {
        this(
                payload,
                partitionCtx,
                properties,
                systemProperties,
                new EnqueuedTimeFactory(enqueuedTimeUtc).enqueuedTime(),
                offset
        );
    }

    public EventImpl(
            final String payload,
            final Map<String, Object> partitionCtx,
            final Map<String, Object> properties,
            final Map<String, Object> systemProperties,
            final EnqueuedTime enqueuedTimeUtc,
            final String offset
    ) {
        this.payload = payload;
        this.partitionCtx = partitionCtx;
        this.properties = properties;
        this.systemProperties = systemProperties;
        this.enqueuedTimeUtc = enqueuedTimeUtc;
        this.offset = offset;
    }

    @Override
    public ParsedEvent parsedEvent() {
        try {
            final JsonStructure jsonStructure = parseJson();
            return new JSONEvent(this, jsonStructure);
        }
        catch (final JsonParsingException ignored) {
            return new PlainEvent(this);
        }
    }

    private JsonStructure parseJson() throws JsonParsingException {
        try (
                final StringReader stringReader = new StringReader(payload); final JsonReader jsonReader = Json.createReader(stringReader)
        ) {
            return jsonReader.read();
        }
    }

    public String payload() {
        return payload;
    }

    public Map<String, Object> partitionCtx() {
        return partitionCtx;
    }

    public Map<String, Object> properties() {
        return properties;
    }

    public Map<String, Object> systemProperties() {
        return systemProperties;
    }

    public EnqueuedTime enqueuedTimeUtc() {
        // If ctor with EnqueuedTime type is provided with null:
        if (enqueuedTimeUtc == null) {
            return new EnqueuedTimeStub();
        }
        return enqueuedTimeUtc;
    }

    public String offset() {
        return offset;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        EventImpl event = (EventImpl) o;
        return Objects.equals(payload, event.payload) && Objects.equals(partitionCtx, event.partitionCtx) && Objects
                .equals(properties, event.properties) && Objects.equals(systemProperties, event.systemProperties)
                && Objects.equals(enqueuedTimeUtc, event.enqueuedTimeUtc) && Objects.equals(offset, event.offset);
    }

    @Override
    public int hashCode() {
        return Objects.hash(payload, partitionCtx, properties, systemProperties, enqueuedTimeUtc, offset);
    }
}
