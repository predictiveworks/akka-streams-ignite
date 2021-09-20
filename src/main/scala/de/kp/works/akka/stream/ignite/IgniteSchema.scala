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

import com.typesafe.config.Config

object FieldTypes extends Enumeration {

  type FieldType = Value

  val BOOLEAN: FieldTypes.Value = Value(1, "BOOLEAN")
  val DATE: FieldTypes.Value    = Value(2, "DATE")
  val DOUBLE: FieldTypes.Value  = Value(3, "DOUBLE")

  def toJava(`type`:FieldType):String = {
    `type` match {
      case BOOLEAN =>
        "java.lang.Boolean"
      case DATE =>
        "java.sql.Date"
      case DOUBLE =>
        "java.lang.Double"
      // TODO
      case _ => throw new Exception(s"Unknown field type detected.")
    }
  }

}

object IgniteSchema {

  def schemaOf(fields:List[IgniteField]):IgniteSchema =
    new IgniteSchema(fields)

  def schemaOf(config:Config):IgniteSchema =
    new IgniteSchema(config2Fields(config))

  private def config2Fields(config:Config):List[IgniteField] = ???

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
