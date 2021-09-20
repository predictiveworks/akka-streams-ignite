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

import de.kp.works.akka.stream.ignite.FieldTypes.FieldType

class IgniteField(fieldName:String, fieldType:FieldType) {

  private var subType:FieldType = _

  def getName:String = fieldName
  /*
   * This is the Java representation of the field
   * data type, e.g. java.lang.String
   */
  def getJavaType:String = FieldTypes.toJava(fieldType)

  def getType:FieldType = fieldType

  def isArray:Boolean = fieldType == FieldTypes.ARRAY

  def setSubType(sType:FieldType):Unit = {
    subType = sType
  }

  def getSubType:FieldType = subType

}
