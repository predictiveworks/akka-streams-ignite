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

import com.typesafe.config.{Config, ConfigObject}

import scala.collection.JavaConversions._

/**
 * The current implementation supports more or less
 * primitive data types, as this is compliant with
 * the ones supported by Apache Ignite.
 *
 * An exception is ARRAY, but this data type is mapped
 * onto its serialized [STRING] value.
 */
object FieldTypes extends Enumeration {

  type FieldType = Value

  val ARRAY: FieldTypes.Value     = Value(0, "ARRAY")
  val BOOLEAN: FieldTypes.Value   = Value(1, "BOOLEAN")
  val DATE: FieldTypes.Value      = Value(2, "DATE")
  val DOUBLE: FieldTypes.Value    = Value(3, "DOUBLE")
  val FLOAT: FieldTypes.Value     = Value(4, "FLOAT")
  val INT: FieldTypes.Value       = Value(5, "INT")
  val LONG: FieldTypes.Value      = Value(6, "LONG")
  val SHORT: FieldTypes.Value     = Value(7, "SHORT")
  val STRING: FieldTypes.Value    = Value(8, "STRING")
  val TIMESTAMP: FieldTypes.Value = Value(9, "TIMESTAMP")

  def toJava(`type`:FieldType):String = {
    `type` match {
      /*
       * Primitive data types
       */
      case BOOLEAN =>
        "java.lang.Boolean"
      case DATE =>
        "java.sql.Date"
      case DOUBLE =>
        "java.lang.Double"
      case FLOAT =>
        "java.lang.Float"
      case INT =>
        "java.lang.Integer"
      case LONG =>
        "java.lang.Long"
      case SHORT =>
        "java.lang.Short"
      case STRING =>
        "java.lang.String"
      case TIMESTAMP =>
        "java.sql.Timestamp"
      /*
       * Complex data types: the current implementation
       * supports complex data types in form of their
       * serialized representation
       */
      case ARRAY =>
        "java.lang.String"
      case _ => throw new Exception(s"Unknown field type detected.")
    }
  }

}

object IgniteSchema {

  def schemaOf(fields:List[IgniteField]):IgniteSchema =
    new IgniteSchema(fields)

  def schemaOf(config:Config):IgniteSchema =
    new IgniteSchema(config2Fields(config))
  /**
   * A helper method to build the list of schema fields
   * from the fields defined in the configuration file.
   */
  private def config2Fields(config:Config):List[IgniteField] = {

    val fields = config.getList("fields")
    fields.map {
      case cobj: ConfigObject =>

        val conf = cobj.toConfig

        val fieldName = conf.getString("name")
        val fieldType = FieldTypes.withName(conf.getString("type"))

        val field = new IgniteField(fieldName, fieldType)

        if (fieldType == FieldTypes.ARRAY) {
          val subType = FieldTypes.withName(conf.getString("subtype"))
          field.setSubType(subType)
        }

        field

      case _ => throw new Exception(s"Field is not specified as configuration object.")

    }
    .toList

  }

}

class IgniteSchema(fields:List[IgniteField]) {

  private val fieldMap = fields
    .map(field => (field.getName, field))
    .toMap

  def getField(fieldName:String): IgniteField = {
    if (fieldMap.contains(fieldName))
      fieldMap(fieldName)

    else
      throw new Exception(s"Provided field name `$fieldName` is unknown.")

  }

  def getFields:List[IgniteField] = fields

}
