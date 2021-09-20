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

import akka.annotation.InternalApi
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler, StageLogging}
import de.kp.works.akka.stream.ignite.{IgniteWriteMessage, IgniteWriteSettings}

import scala.collection.immutable

/**
 * INTERNAL API
 */
@InternalApi
private[ignite] class IgniteFlowStage[T, C](settings: IgniteWriteSettings)
  extends GraphStage[FlowShape[immutable.Seq[IgniteWriteMessage[T, C]], immutable.Seq[IgniteWriteMessage[T, C]]]] {

  private val in = Inlet[immutable.Seq[IgniteWriteMessage[T, C]]]("IgniteFlow.in")
  private val out = Outlet[immutable.Seq[IgniteWriteMessage[T, C]]]("IgniteFlow.out")

  override def shape: FlowShape[immutable.Seq[IgniteWriteMessage[T, C]], immutable.Seq[IgniteWriteMessage[T, C]]] =
    FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new IgniteStageLogic()

  private class IgniteStageLogic extends GraphStageLogic(shape) with InHandler with OutHandler with StageLogging {
    /*
     * Ignite connection handling: create a new instance
     * of [IgniteClient] before this stage starts, and,
     * close the respective client after this stage stopped.
     */
    private var client:IgniteClient = _

    override def preStart(): Unit =
      client = new IgniteClient(settings)

    override def postStop(): Unit =
      client.close()

    setHandlers(in, out, this)

    /**
     * Write messages to Apache Ignite leveraging
     * the [IgniteClient]
     */
    def write(messages: immutable.Seq[IgniteWriteMessage[T, C]]):Unit =
      if (client != null && !client.isClosed)
        client.write(messages)

    override def onPush(): Unit = {

      /* Retrieve new messages */
      val messages = grab(in)

      /* Write messages to Apache Ignite */
      if (messages.nonEmpty) {

        write(messages)
        push(out, messages)

      }

      tryPull(in)

    }

    override def onPull(): Unit = tryPull()

    private def tryPull(): Unit =
      if (!isClosed(in) && !hasBeenPulled(in)) {
        pull(in)
      }
  }
}
