package de.kp.works.akka.stream.ignite
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

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.ignite.configuration.{DataRegionConfiguration, DataStorageConfiguration, IgniteConfiguration}
import org.apache.ignite.logger.java.JavaLogger
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi
import org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.TcpDiscoveryMulticastIpFinder

class IgniteWriteSettings {

  private val path = "reference.conf"
  /**
   * This is the reference to the overall configuration
   * file that holds all configuration required for this
   * application
   */
  private val cfg: Config = ConfigFactory
    .load(path).getConfig("akka-streams-ignite")
  /**
   * The auto flush frequency of the stream buffer is
   * internally set to 0.5 sec (500 ms)
   */
  def getAutoFlushFrequency:Int =
    cfg.getInt("autoFlushFrequency")

  def getCacheName:String =
    cfg.getString("cacheName")

  def getSchema:IgniteSchema =
    IgniteSchema.schemaOf(cfg.getConfig("schema"))

  def getTimeWindow:Int =
    cfg.getInt("timeWindow")

  /**
   * A helper method to represent these settings as
   * Apache Ignite configuration
   */
  def toConfiguration:IgniteConfiguration = {

    val igniteCfg = new IgniteConfiguration
    /*
     * Configure default java logger which leverages
     * file config/java.util.logging.properties
     */
    val logger = new JavaLogger()
    igniteCfg.setGridLogger(logger)
    /*
     * Configuring the data storage
     */
    val dataStorageCfg = new DataStorageConfiguration
    /*
     * Default memory region that grows endlessly. Any cache will
     * be bound to this memory region unless another region is set
     * in the cache's configuration.
     */
    val dataRegionCfg = new DataRegionConfiguration
    dataRegionCfg.setName("Default_Region")
    /*
     * 100 MB memory region
     */
    dataRegionCfg.setInitialSize(100 * 1024 * 1024)
    dataStorageCfg.setSystemRegionMaxSize(2L * 1024 * 1024 * 1024)

    igniteCfg.setDataStorageConfiguration(dataStorageCfg)
    /*
     * Explicitly configure TCP discovery SPI to provide
     * list of initial nodes.
     */
    val discoverySpi = new TcpDiscoverySpi
    /*
     * Ignite provides several options for automatic discovery
     * that can be used instead os static IP based discovery.
     */
    val ipFinder = new TcpDiscoveryMulticastIpFinder

    val addresses = cfg.getString("addresses")
    ipFinder.setAddresses(java.util.Arrays.asList(addresses))

    discoverySpi.setIpFinder(ipFinder)
    igniteCfg.setDiscoverySpi(discoverySpi)

    igniteCfg

  }

}
