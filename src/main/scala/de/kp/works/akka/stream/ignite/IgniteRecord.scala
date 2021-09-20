package de.kp.works.akka.stream.ignite

import de.kp.works.akka.stream.ignite.FieldTypes

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

class IgniteRecord(schema:IgniteSchema, fields:Map[String,Any]) extends Serializable {

  /**
   * Get the value of a field in the record.
   */
  def get(fieldName:String):Any = {
    fields.get(fieldName)
  }

  def getAsString(fieldName:String):String = {

    val field = schema.getField(fieldName)
    val value = fields(fieldName)

    field.getType match {
      /*
       * Primitive data types
       */
      case FieldTypes.BOOLEAN =>
        value.asInstanceOf[Boolean].toString
      case FieldTypes.DATE =>
        value.asInstanceOf[java.sql.Date].toString
      case FieldTypes.DOUBLE =>
        value.asInstanceOf[Double].toString
      case FieldTypes.FLOAT =>
        value.asInstanceOf[Float].toString
      case FieldTypes.INT =>
        value.asInstanceOf[Int].toString
      case FieldTypes.LONG =>
        value.asInstanceOf[Long].toString
      case FieldTypes.SHORT =>
        value.asInstanceOf[Short].toString
      case FieldTypes.STRING =>
        value.asInstanceOf[String]
      case FieldTypes.TIMESTAMP =>
        value.asInstanceOf[java.sql.Timestamp].toString
      /*
       * Complex data types: the current implementation supports
       * Array data types in form of their serialized representation
       */
      case FieldTypes.ARRAY =>
        val subType = field.getSubType
        value.asInstanceOf[List[_]].map(v => {
          subType match {
            case FieldTypes.BOOLEAN =>
              v.asInstanceOf[Boolean].toString
            case FieldTypes.DATE =>
              v.asInstanceOf[java.sql.Date].toString
            case FieldTypes.DOUBLE =>
              v.asInstanceOf[Double].toString
            case FieldTypes.FLOAT =>
              v.asInstanceOf[Float].toString
            case FieldTypes.INT =>
              v.asInstanceOf[Int].toString
            case FieldTypes.LONG =>
              v.asInstanceOf[Long].toString
            case FieldTypes.SHORT =>
              v.asInstanceOf[Short].toString
            case FieldTypes.STRING =>
              v.asInstanceOf[String]
            case FieldTypes.TIMESTAMP =>
              v.asInstanceOf[java.sql.Timestamp].toString
            case _ => throw new Exception(s"Unknown field subtype detected.")
          }
        }).mkString(",")

      case _ => throw new Exception(s"Unknown field type detected.")
    }

  }
  /**
   * Get the schema of the record.
   */
  def getSchema:IgniteSchema = schema

}
