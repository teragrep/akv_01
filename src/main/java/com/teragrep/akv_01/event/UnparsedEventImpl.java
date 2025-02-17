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
import com.teragrep.akv_01.event.metadata.partitionContext.EventPartitionContext;
import com.teragrep.akv_01.event.metadata.properties.EventProperties;
import com.teragrep.akv_01.event.metadata.systemProperties.EventSystemProperties;
import com.teragrep.akv_01.event.metadata.time.EnqueuedTime;
import jakarta.json.Json;
import jakarta.json.JsonReader;
import jakarta.json.JsonStructure;
import jakarta.json.stream.JsonParsingException;

import java.io.StringReader;
import java.util.Objects;

public final class UnparsedEventImpl implements UnparsedEvent {

    private final String payload;
    private final EventPartitionContext partitionCtx;
    private final EventProperties eventProperties;
    private final EventSystemProperties eventSystemProperties;
    private final EnqueuedTime enqueuedTimeUtc;
    private final EventOffset eventOffset;

    public UnparsedEventImpl(
            final String payload,
            final EventPartitionContext partitionCtx,
            final EventProperties properties,
            final EventSystemProperties systemProperties,
            final EnqueuedTime enqueuedTimeUtc,
            final EventOffset offset
    ) {
        this.payload = payload;
        this.partitionCtx = partitionCtx;
        this.eventProperties = properties;
        this.eventSystemProperties = systemProperties;
        this.enqueuedTimeUtc = enqueuedTimeUtc;
        this.eventOffset = offset;
    }

    public JsonStructure parseJson() throws JsonParsingException {
        try (
                final StringReader stringReader = new StringReader(payload); final JsonReader jsonReader = Json.createReader(stringReader)
        ) {
            return jsonReader.read();
        }
    }

    public String payload() {
        return payload;
    }

    public EventPartitionContext partitionCtx() {
        return partitionCtx;
    }

    public EventProperties properties() {
        return eventProperties;
    }

    public EventSystemProperties systemProperties() {
        return eventSystemProperties;
    }

    public EnqueuedTime enqueuedTimeUtc() {
        return enqueuedTimeUtc;
    }

    public EventOffset offset() {
        return eventOffset;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        UnparsedEventImpl event = (UnparsedEventImpl) o;
        return Objects.equals(payload, event.payload) && Objects.equals(partitionCtx, event.partitionCtx)
                && Objects.equals(eventProperties, event.eventProperties) && Objects.equals(eventSystemProperties, event.eventSystemProperties) && Objects.equals(enqueuedTimeUtc, event.enqueuedTimeUtc) && Objects.equals(eventOffset, event.eventOffset);
    }

    @Override
    public int hashCode() {
        return Objects
                .hash(payload, partitionCtx, eventProperties, eventSystemProperties, enqueuedTimeUtc, eventOffset);
    }
}
