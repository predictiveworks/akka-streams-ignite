package de.kp.works.akka.stream.ignite.impl
/*
 * Copyright (c) 20129 - 2021 Dr. Krusche & Partner PartG. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 * @author Stefan Krusche, Dr. Krusche & Partner PartG
 *
 */

import de.kp.works.akka.stream.ignite.IgniteRecord
import org.apache.ignite.IgniteLogger
import org.apache.ignite.stream.StreamAdapter

trait IgniteStreamer {

  def streamRecord(record:IgniteRecord):Unit

}

class IgniteRecordStreamer[K,V] extends StreamAdapter[IgniteRecord, K, V] with IgniteStreamer {

  /** Logger */
  private val log:IgniteLogger = getIgnite.log()

  /**
   * This method defines the bridge between records
   * that flow in and the respective Apache Ignite
   * cache
   */
  override def streamRecord(record: IgniteRecord): Unit = {
    /*
     * The leveraged extractors below must be explicitly
     * defined when initiating this streamer
     */
    if (getMultipleTupleExtractor != null) {

      val entries:java.util.Map[K,V] = getMultipleTupleExtractor.extract(record)
      if (log.isTraceEnabled)
        log.trace("Adding cache entries: " + entries)

      getStreamer.addData(entries)

    }
    else {

      val entry:java.util.Map.Entry[K,V] = getSingleTupleExtractor.extract(record)
      if (log.isTraceEnabled)
        log.trace("Adding cache entry: " + entry)

      getStreamer.addData(entry)

    }

  }

}
