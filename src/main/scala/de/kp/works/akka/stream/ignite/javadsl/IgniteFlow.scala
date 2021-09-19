package de.kp.works.akka.stream.ignite.javadsl
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
import akka.stream.javadsl.Flow
import akka.stream.scaladsl.{Flow => ScalaFlow}

import de.kp.works.akka.stream.ignite.{IgniteRecord, IgniteWriteMessage, IgniteWriteSettings}
import de.kp.works.akka.stream.ignite.scaladsl.{IgniteFlow => ScalaIgniteFlow}
import java.util.{List => JList}
import scala.collection.JavaConverters._

object IgniteFlow {
  /**
   * Flow to write `IgniteRecord`s to Apache Ignite,
   * elements within one list are stored within one
   * transaction.
   */
  def create(
    settings: IgniteWriteSettings): Flow[JList[IgniteWriteMessage[IgniteRecord, NotUsed]],
    JList[IgniteWriteMessage[IgniteRecord, NotUsed]],
    NotUsed] =
    ScalaFlow[JList[IgniteWriteMessage[IgniteRecord, NotUsed]]]
      .map(_.asScala.toList)
      .via(ScalaIgniteFlow.create(settings))
      .map(_.asJava)
      .asJava

}
