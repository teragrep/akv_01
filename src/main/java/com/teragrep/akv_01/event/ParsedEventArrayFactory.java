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

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public final class ParsedEventArrayFactory {

    private final String[] payloads;
    private final Map<String, Object> partitionCtx;
    private final Map<String, Object>[] propertiesArray;
    private final Map<String, Object>[] systemPropertiesArray;
    private final List<Object> enqueuedTimeUtcList;
    private final List<String> offsetList;

    public ParsedEventArrayFactory(
            final String[] payloads,
            final Map<String, Object> partitionCtx,
            final Map<String, Object>[] propertiesArray,
            final Map<String, Object>[] systemPropertiesArray,
            final List<Object> enqueuedTimeUtcList,
            final List<String> offsetList
    ) {
        this.payloads = payloads;
        this.partitionCtx = partitionCtx;
        this.propertiesArray = propertiesArray;
        this.systemPropertiesArray = systemPropertiesArray;
        this.enqueuedTimeUtcList = enqueuedTimeUtcList;
        this.offsetList = offsetList;
    }

    public ParsedEvent[] asArray() {
        final ParsedEvent[] events = new ParsedEvent[payloads.length];
        for (int i = 0; i < payloads.length; i++) {
            events[i] = new EventImpl(
                    payloads[i],
                    partitionCtx,
                    propertiesArray[i],
                    systemPropertiesArray[i],
                    enqueuedTimeUtcList.get(i),
                    offsetList.get(i)
            ).parsedEvent();
        }
        return events;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ParsedEventArrayFactory that = (ParsedEventArrayFactory) o;
        return Objects.deepEquals(payloads, that.payloads) && Objects.equals(partitionCtx, that.partitionCtx)
                && Objects.deepEquals(propertiesArray, that.propertiesArray) && Objects.deepEquals(systemPropertiesArray, that.systemPropertiesArray) && Objects.equals(enqueuedTimeUtcList, that.enqueuedTimeUtcList) && Objects.equals(offsetList, that.offsetList);
    }

    @Override
    public int hashCode() {
        return Objects
                .hash(Arrays.hashCode(payloads), partitionCtx, Arrays.hashCode(propertiesArray), Arrays.hashCode(systemPropertiesArray), enqueuedTimeUtcList, offsetList);
    }
}
