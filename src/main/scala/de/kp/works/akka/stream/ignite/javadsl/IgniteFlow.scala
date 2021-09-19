package de.kp.works.akka.stream.ignite.javadsl

import akka.NotUsed
import akka.stream.javadsl.Flow
import de.kp.works.akka.stream.ignite.{IgniteRecord, IgniteWriteMessage, IgniteWriteSettings}

import java.util.{List => JList}

object IgniteFlow {
  /**
   * Flow to write `IgniteRecord`s to Apache Ignite,
   * elements within one list are stored within one
   * transaction.
   */
  def create(
    settings: IgniteWriteSettings): Flow[JList[IgniteWriteMessage[IgniteRecord, NotUsed]],
    JList[IgniteWriteMessage[IgniteRecord, NotUsed]],
    NotUsed] = ???

}
