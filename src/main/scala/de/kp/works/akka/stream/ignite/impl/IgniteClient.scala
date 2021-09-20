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

import de.kp.works.akka.stream.ignite.{FieldTypes, IgniteRecord, IgniteWriteMessage, IgniteWriteSettings}
import org.apache.ignite.{Ignite, IgniteCache, Ignition}
import org.apache.ignite.binary.BinaryObject
import org.apache.ignite.cache.QueryEntity
import org.apache.ignite.configuration.CacheConfiguration
import org.apache.ignite.stream.StreamSingleTupleExtractor

import java.security.MessageDigest
import scala.collection.JavaConversions._
import scala.collection.{immutable, mutable}
import java.util.{ArrayList => JArrayList}
import javax.cache.configuration.FactoryBuilder
import javax.cache.expiry.{CreatedExpiryPolicy, Duration}
import scala.concurrent.duration.SECONDS

class IgniteClient(settings:IgniteWriteSettings) {

  private var ignite:Ignite = _

  private var cache:IgniteCache[String,BinaryObject] = _
  private var streamer:IgniteRecordStreamer[String,BinaryObject] = _
  /*
   * The (temporary) streaming cache name
   */
  private val cacheName = settings.getCacheName

  /**
   * Initialize ignite connection, cache and streamer
   * as the backbone of this Apache Ignite client
   */
  buildClient()

  def write[T, C](messages: immutable.Seq[IgniteWriteMessage[T, C]]):Unit = {

    if (ignite == null || streamer == null)
      throw new Exception(s"No connection to Apache Ignite initialized.")
    /*
     * Start transaction
     */
    val tx = ignite.transactions.txStart
    try {

      messages.foreach {
        case IgniteWriteMessage(record: IgniteRecord, _) =>
          streamer.streamRecord(record)
        case _ =>
          throw new Exception(s"Unknown message format detected.")
       }

      tx.commit()

    } catch {
      case _:Throwable => tx.rollback()
    }
  }

  def close():Unit = {
    if (ignite != null) {
      ignite.close()
      ignite = null
    }
  }

  def isClosed:Boolean = {
    if (ignite == null) true else false
  }

  /*********************
   *
   * INITIALIZATION
   *
   *********************/

  private def buildClient():Unit = {
    try {
      /*
       * STEP #1: Initialize Ignite connection
       * from the provided settings
       */
      ignite = Ignition.getOrStart(settings.toConfiguration)
      /*
       * STEP #2: Delete if exists and then create
       * (temporary) streaming cache
       */
      cache = buildCache
      /*
       * STEP #3: Build
       */
      streamer = buildStreamer()

    } catch {
      case _:Throwable => /* Do nothing */
    }

  }

  private def buildCache:IgniteCache[String,BinaryObject] = {
    /*
     * The cache is configured with sliding window holding
     * N seconds of the streaming data; note, that we delete
     * an already equal named cache
     */
    deleteCache()

    val config = buildCacheConfig
    ignite.getOrCreateCache(config)

  }

  /**
   * Configuration for the events cache to store the stream
   * of events. This cache is configured with a sliding window
   * of N seconds, which means that data older than N second
   * will be automatically removed from the cache.
   */
  private def buildCacheConfig:CacheConfiguration[String,BinaryObject] = {
    /*
     * Defining query entities is the Apache Ignite
     * mechanism to dynamically define a queryable
     * 'class'
     */
    val qes = new JArrayList[QueryEntity]()
    qes.add(buildQueryEntity)
    /*
     * Configure streaming cache, an Ignite cache used
     * to persist SSE temporarily
     */
    val cfg = new CacheConfiguration[String,BinaryObject]()
    cfg.setName(cacheName)
    /*
     * Specify Apache Ignite cache configuration; it is
     * important to leverage 'BinaryObject' as well as
     * 'setStoreKeepBinary'
     */
    cfg.setStoreKeepBinary(true)
    cfg.setIndexedTypes(classOf[String],classOf[BinaryObject])

    cfg.setQueryEntities(qes)

    cfg.setStatisticsEnabled(true)

    /* Sliding window of 'timeWindow' in seconds */
    val duration = settings.getTimeWindow / 1000

    cfg.setExpiryPolicyFactory(
      FactoryBuilder.factoryOf(new CreatedExpiryPolicy(new Duration(SECONDS, duration))))

    cfg

  }
  /**
   * This method deletes the (temporary) streaming
   * cache from the Ignite cluster
   */
  private def deleteCache():Unit = {
    try {

      if (ignite.cacheNames().contains(cacheName)) {
        val cache = ignite.cache(cacheName)
        cache.destroy()
      }

    } catch {
      case _:Throwable => /* do noting */

    }
  }

  private def buildStreamer():IgniteRecordStreamer[String,BinaryObject] = {

    val dataStreamer = ignite.dataStreamer[String,BinaryObject](cacheName)
    /*
     * allowOverwrite(boolean) - Sets flag enabling overwriting
     * existing values in cache. Data streamer will perform better
     * if this flag is disabled, which is the default setting.
     */
    dataStreamer.allowOverwrite(false)
    /*
     * IgniteDataStreamer buffers the data and most likely it just
     * waits for buffers to fill up. We set the time interval after
     * which buffers will be flushed even if they are not full
     */
    val autoFlushFrequency = settings.getAutoFlushFrequency
    dataStreamer.autoFlushFrequency(autoFlushFrequency)
    /*
     * Build specific [IgniteStreamer] instance
     */
    val igniteStreamer = new IgniteRecordStreamer[String,BinaryObject]()

    igniteStreamer.setIgnite(ignite)
    igniteStreamer.setStreamer(dataStreamer)
    /*
     * The extractor is the linking element between the Flow
     * messages and their specification as Apache Ignite cache
     * entries.
     *
     * We currently leverage a single tuple extractor as we do
     * not have experience whether we should introduce multiple
     * tuple extraction. Additional performance requirements can
     * lead to a channel in the selected extractor
     */
    val extractor = createExtractor
    igniteStreamer.setSingleTupleExtractor(extractor)

    igniteStreamer

  }
  /**
   * The [IgniteClient] is backed by a single tuple extractor
   */
  private def createExtractor: StreamSingleTupleExtractor[IgniteRecord, String, BinaryObject] = {

    new StreamSingleTupleExtractor[IgniteRecord,String,BinaryObject]() {

      override def extract(event:IgniteRecord):java.util.Map.Entry[String,BinaryObject] = {

        val entries = scala.collection.mutable.HashMap.empty[String,BinaryObject]
        try {

          val (cacheKey, cacheValue) = buildEntry(event)
          entries.put(cacheKey,cacheValue)

        } catch {
          case e:Exception => e.printStackTrace()
        }
        entries.entrySet().iterator().next

      }
    }

  }

  /**
   * A helper method to build an Apache Ignite QueryEntity;
   * this entity reflects the format of an [IgniteRecord]
   */
  private def buildQueryEntity:QueryEntity = {

    val queryEntity = new QueryEntity()

    queryEntity.setKeyType("java.lang.String")
    queryEntity.setValueType(cacheName)

    val fields = buildFields()

    queryEntity.setFields(fields)
    queryEntity

  }

  /*******************************
   *
   * SEMANTIC SPECIFICATION
   *
   ******************************/

  private def buildFields():java.util.LinkedHashMap[String,String] = {

    val schema = settings.getSchema
    val fields = new java.util.LinkedHashMap[String,String]()

    schema.getFields.foreach(field =>
      fields.put(field.getName, field.getJavaType)
    )

    fields

  }

  /**
   * This method determines how to represent an [IgniteRecord]
   * as the respective Apache Ignite cache entry
   */
  private def buildEntry(record:IgniteRecord):(String, BinaryObject) = {

    val keyparts = mutable.ArrayBuffer.empty[String]
    val builder = ignite.binary().builder(cacheName)
    /*
     * In order to guarantee the respective field order,
     * the record schema is used
     */
    val fields = record.getSchema.getFields
    fields.foreach(field => {
      val fieldName = field.getName
      val fieldType = field.getType
      /*
       * Primitive data types are mapped onto their
       * Java class representation
       */
      if (fieldType == FieldTypes.ARRAY) {
        /*
         * Complex data types are registered in a
         * serialized representation
         */
        builder.setField(fieldName, record.getAsString(fieldName))

      } else
        builder.setField(fieldName, record.get(fieldName))

      keyparts += record.getAsString(fieldName)
    })

    val cacheValue = builder.build()
    /*
     * The cache key is built from the content
     * to enable the detection of duplicates.
     */
    val serialized = keyparts.mkString("#")

    val cacheKey = new String(MessageDigest.getInstance("MD5")
      .digest(serialized.getBytes("UTF-8")))

    (cacheKey, cacheValue)

  }

}
