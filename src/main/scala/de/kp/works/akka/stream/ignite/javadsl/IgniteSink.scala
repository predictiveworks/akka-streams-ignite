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

import akka.{Done, NotUsed}
import akka.stream.javadsl.{Keep, Sink}
import de.kp.works.akka.stream.ignite.{IgniteRecord, IgniteWriteMessage, IgniteWriteSettings}

import java.util.concurrent.CompletionStage
import java.util.{List => JList}

object IgniteSink {
  /**
   * Create a sink sending records to Apache Ignite.
   *
   * The materialized value completes on stream completion.
   */
  def create(
    settings:IgniteWriteSettings):Sink[JList[IgniteWriteMessage[IgniteRecord, NotUsed]], CompletionStage[Done]] =
      IgniteFlow
        .create(settings)
        .toMat(Sink.ignore[JList[IgniteWriteMessage[IgniteRecord, NotUsed]]], Keep.right[NotUsed, CompletionStage[Done]])


}
