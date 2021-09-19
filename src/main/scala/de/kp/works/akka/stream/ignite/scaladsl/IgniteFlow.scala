package de.kp.works.akka.stream.ignite.scaladsl
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

import akka.NotUsed
import akka.stream.scaladsl.Flow
import de.kp.works.akka.stream.ignite.impl.IgniteFlowStage
import de.kp.works.akka.stream.ignite.{IgniteRecord, IgniteWriteMessage, IgniteWriteSettings}

import scala.collection.immutable

object IgniteFlow {
  /**
   * Flow to write `IgniteRecord`s to Apache Ignite,
   * elements within one list are stored within one
   * transaction.
   */
  def create(
    settings: IgniteWriteSettings): Flow[immutable.Seq[IgniteWriteMessage[IgniteRecord, NotUsed]],
    immutable.Seq[IgniteWriteMessage[IgniteRecord, NotUsed]], NotUsed] =
    Flow
      .fromGraph(new IgniteFlowStage[IgniteRecord, NotUsed](settings))

}
