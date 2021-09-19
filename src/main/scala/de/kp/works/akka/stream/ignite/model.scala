package de.kp.works.akka.stream.ignite

import akka.NotUsed

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

/**
 * The current implementation does not support `passThrough`.
 */
object IgniteWriteMessage {

  /** Apply method to use when not using passThrough */

  def apply[T](record: T): IgniteWriteMessage[T, NotUsed] =
    IgniteWriteMessage(record, NotUsed)

  /** Java-api - without passThrough */

  def create[T](record: T): IgniteWriteMessage[T, NotUsed] =
    IgniteWriteMessage(record, NotUsed)
}

final case class IgniteWriteMessage[T, C](record: T, passThrough: C)
