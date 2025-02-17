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
import com.teragrep.akv_01.event.metadata.offset.EventOffsetStub;
import com.teragrep.akv_01.event.metadata.partitionContext.EventPartitionContext;
import com.teragrep.akv_01.event.metadata.partitionContext.EventPartitionContextImpl;
import com.teragrep.akv_01.event.metadata.partitionContext.EventPartitionContextStub;
import com.teragrep.akv_01.event.metadata.properties.EventProperties;
import com.teragrep.akv_01.event.metadata.properties.EventPropertiesImpl;
import com.teragrep.akv_01.event.metadata.properties.EventPropertiesStub;
import com.teragrep.akv_01.event.metadata.systemProperties.EventSystemProperties;
import com.teragrep.akv_01.event.metadata.systemProperties.EventSystemPropertiesImpl;
import com.teragrep.akv_01.event.metadata.systemProperties.EventSystemPropertiesStub;
import com.teragrep.akv_01.event.metadata.time.EnqueuedTime;
import com.teragrep.akv_01.event.metadata.time.EnqueuedTimeImpl;
import com.teragrep.akv_01.event.metadata.time.EnqueuedTimeStub;

import java.util.*;

public final class ParsedEventListFactory {

    private final String[] payloads;
    private final Map<String, Object> partitionCtx;
    private final Map<String, Object>[] propertiesArray;
    private final Map<String, Object>[] systemPropertiesArray;
    private final List<Object> enqueuedTimeUtcList;
    private final List<String> offsetList;

    private static final EventPartitionContext eventPartitionContextStub = new EventPartitionContextStub();
    private static final EventProperties eventPropertiesStub = new EventPropertiesStub();
    private static final EventSystemProperties eventSystemPropertiesStub = new EventSystemPropertiesStub();
    private static final EnqueuedTime enqueuedTimeStub = new EnqueuedTimeStub();
    private static final EventOffset eventOffsetStub = new EventOffsetStub();

    public ParsedEventListFactory(
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

    public List<ParsedEvent> asList() {
        final List<ParsedEvent> events = new ArrayList<>(payloads.length);
        for (int i = 0; i < payloads.length; i++) {
            if (payloads[i] != null) {
                EventPartitionContext eventPartitionContext = eventPartitionContextStub;
                EventProperties eventProperties = eventPropertiesStub;
                EventSystemProperties eventSystemProperties = eventSystemPropertiesStub;
                EnqueuedTime enqueuedTime = enqueuedTimeStub;
                EventOffset eventOffset = eventOffsetStub;

                if (partitionCtx != null) {
                    eventPartitionContext = new EventPartitionContextImpl(partitionCtx);
                }

                if (propertiesArray != null && propertiesArray[i] != null) {
                    eventProperties = new EventPropertiesImpl(propertiesArray[i]);
                }

                if (systemPropertiesArray != null && systemPropertiesArray[i] != null) {
                    eventSystemProperties = new EventSystemPropertiesImpl(systemPropertiesArray[i]);
                }

                if (enqueuedTimeUtcList != null && enqueuedTimeUtcList.get(i) != null) {
                    enqueuedTime = new EnqueuedTimeImpl(enqueuedTimeUtcList.get(i));
                }

                if (offsetList != null && offsetList.get(i) != null) {
                    eventOffset = new EventOffsetImpl(offsetList.get(i));
                }

                events
                        .add(
                                new ParsedEventFactory(
                                        new UnparsedEventImpl(
                                                payloads[i],
                                                eventPartitionContext,
                                                eventProperties,
                                                eventSystemProperties,
                                                enqueuedTime,
                                                eventOffset
                                        )
                                ).parsedEvent()
                        );
            }
        }
        return events;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ParsedEventListFactory that = (ParsedEventListFactory) o;
        return Objects.deepEquals(payloads, that.payloads) && Objects.equals(partitionCtx, that.partitionCtx)
                && Objects.deepEquals(propertiesArray, that.propertiesArray) && Objects.deepEquals(systemPropertiesArray, that.systemPropertiesArray) && Objects.equals(enqueuedTimeUtcList, that.enqueuedTimeUtcList) && Objects.equals(offsetList, that.offsetList);
    }

    @Override
    public int hashCode() {
        return Objects
                .hash(Arrays.hashCode(payloads), partitionCtx, Arrays.hashCode(propertiesArray), Arrays.hashCode(systemPropertiesArray), enqueuedTimeUtcList, offsetList);
    }
}
