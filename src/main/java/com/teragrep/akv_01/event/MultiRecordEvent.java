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

import jakarta.json.JsonArray;
import jakarta.json.JsonValue;

import java.util.List;

public final class MultiRecordEvent {

    private final ParsedEvent parsedEvent;

    public MultiRecordEvent(final ParsedEvent parsedEvent) {
        this.parsedEvent = parsedEvent;
    }

    public boolean isValid() {
        boolean valid = true;
        if (!parsedEvent.isJsonStructure()) {
            // not json structure
            valid = false;
        }

        if (valid && !parsedEvent.asJsonStructure().getValueType().equals(JsonValue.ValueType.OBJECT)) {
            // not json object
            valid = false;
        }

        if (
            valid && (!parsedEvent.asJsonStructure().asJsonObject().containsKey("records") || !parsedEvent
                    .asJsonStructure()
                    .asJsonObject()
                    .get("records")
                    .getValueType()
                    .equals(JsonValue.ValueType.ARRAY))
        ) {
            // no records array
            valid = false;
        }

        if (valid) {
            final JsonArray recordsArray = parsedEvent.asJsonStructure().asJsonObject().getJsonArray("records");
            for (final JsonValue record : recordsArray) {
                if (!record.getValueType().equals(JsonValue.ValueType.OBJECT)) {
                    valid = false;
                    break;
                }
            }
        }

        return valid;
    }

    public List<ParsedEvent> records() {
        if (!isValid()) {
            throw new IllegalStateException("Event is not a multi record event");
        }

        return parsedEvent
                .asJsonStructure()
                .asJsonObject()
                .getJsonArray("records")
                .getValuesAs(
                        jsonValue -> new ParsedEventFactory(
                                new UnparsedEventImpl(
                                        jsonValue.asJsonObject().toString(),
                                        parsedEvent.partitionCtx(),
                                        parsedEvent.properties(),
                                        parsedEvent.systemProperties(),
                                        parsedEvent.enqueuedTimeUtc(),
                                        parsedEvent.offset()
                                )
                        ).parsedEvent()
                );
    }
}
