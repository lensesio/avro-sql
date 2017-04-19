/*
 * Copyright 2017 Landoop.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.landoop.avro.sql

import java.nio.ByteBuffer
import java.text.SimpleDateFormat
import java.util.{Date, TimeZone}

import org.apache.avro.Conversions.{DecimalConversion, UUIDConversion}
import org.apache.avro.generic.{GenericFixed, IndexedRecord}
import org.apache.avro.{LogicalTypes, Schema}

import scala.collection.JavaConversions._
import scala.collection.mutable.{Map => MutableMap}

object AvroUnpacker {
  private val ISO_DATE_FORMAT: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
  private val TIME_FORMAT: SimpleDateFormat = new SimpleDateFormat("HH:mm:ss.SSSZ")

  ISO_DATE_FORMAT.setTimeZone(TimeZone.getTimeZone("UTC"))

  private val DecimalConversion = new DecimalConversion
  private val UUIDConversion = new UUIDConversion
  private val DECIMAL = "decimal"
  private val UUID = "uuid"
  private val DATE = "date"
  private val TIME_MILLIS = "time-millis"
  private val TIME_MICROS = "time-micros"
  private val TIMESTAMP_MILLIS = "timestamp-millis"
  private val TIMESTAMP_MICROS = "timestamp-micros"

  def fromBytes(value: Any)(implicit schema: Schema): Any = {
    val bytes = value match {
      case b: ByteBuffer => b.array()
      case a: Array[Byte] => a
      case other => throw new IllegalArgumentException(s"${other.getClass.getName} is not supported ")
    }
    Option(LogicalTypes.fromSchemaIgnoreInvalid(schema)).map { lt =>
      lt.getName match {
        case UUID => UUIDConversion.fromCharSequence(new String(bytes), schema, lt)
        case DECIMAL => DecimalConversion.fromBytes(ByteBuffer.wrap(bytes), schema, lt)
        case _ => bytes //not supporting something else
      }
    }.getOrElse(bytes)
  }

  def fromFixed(value: Any)(implicit schema: Schema): Any = fromBytes(value.asInstanceOf[GenericFixed].bytes())

  def fromArray(value: Any)(implicit schema: Schema): Any = {
    value match {
      case c: java.util.Collection[_] => c.map(apply(_, schema.getElementType))
      case arr: Array[_] if arr.getClass.getComponentType.isPrimitive => arr
      case arr: Array[_] => arr.map(apply(_, schema.getElementType))
      case other => throw new IllegalArgumentException(s"Unknown ARRAY type ${other.getClass.getName}")
    }
  }

  def fromMap(value: Any)(implicit schema: Schema): Any = {
    value.asInstanceOf[java.util.Map[_, _]].foldLeft(Map.empty[String, Any]) { case (map, (key, v)) =>
      map + (key.toString -> apply(v, schema.getValueType))
    }
  }

  def fromRecord(value: Any)(implicit schema: Schema): Any = {
    value match {
      case record: IndexedRecord =>
        record.getSchema.getFields
          .zipWithIndex
          .foldLeft(MutableMap.empty[String, Any]) { case (map, (f, i)) =>
            map + (f.name -> apply(record.get(i), f.schema))
          }
      case other => throw new IllegalArgumentException(s"Unsupported RECORD type ${other.getClass.getName}")
    }
  }

  def fromUnion(value: Any)(implicit schema: Schema): Any = {
    schema.getTypes.toList match {
      case actualSchema +: Nil => apply(value, actualSchema)
      case List(n, actualSchema) if n.getType == Schema.Type.NULL => apply(value, actualSchema)
      case List(actualSchema, n) if n.getType == Schema.Type.NULL => apply(value, actualSchema)
      case _ => throw new IllegalArgumentException("Unions has one specific type and null")
    }
  }

  def fromInt(value: Any)(implicit schema: Schema): Any = {
    val i = value.asInstanceOf[Int]
    Option(LogicalTypes.fromSchemaIgnoreInvalid(schema)).map { lt =>
      lt.getName match {
        case TIME_MILLIS => TIME_FORMAT.format(new Date(i.toLong))
        case TIMESTAMP_MILLIS => ISO_DATE_FORMAT.format(new Date(i.toLong))
        case DATE => ISO_DATE_FORMAT.format(new Date(i.toLong * 86400000))
      }
    }.getOrElse(i)
  }


  def fromLong(value: Any)(implicit schema: Schema): Any = {
    val l = value.asInstanceOf[Long]
    Option(LogicalTypes.fromSchemaIgnoreInvalid(schema)).map { lt =>
      lt.getName match {
        case TIME_MILLIS => TIME_FORMAT.format(new Date(l))
        case TIMESTAMP_MILLIS => ISO_DATE_FORMAT.format(new Date(l))
        case DATE => ISO_DATE_FORMAT.format(new Date(l * 86400000))
      }
    }.getOrElse(l)
  }

  def apply(value: Any, schema: Schema): Any = {
    Option(value).map { _ =>
      implicit val s = schema
      schema.getType match {
        case Schema.Type.BOOLEAN | Schema.Type.NULL |
             Schema.Type.DOUBLE | Schema.Type.FLOAT => value

        case Schema.Type.LONG => fromLong(value)
        case Schema.Type.STRING => value.toString //yes UTF8
        case Schema.Type.INT => fromInt(value)
        case Schema.Type.ENUM => value.toString
        case Schema.Type.UNION => fromUnion(value)
        case Schema.Type.ARRAY => fromArray(value)
        case Schema.Type.FIXED => fromFixed(value)
        case Schema.Type.MAP => fromMap(value)
        case Schema.Type.BYTES => fromBytes(value)
        case Schema.Type.RECORD => fromRecord(value)
        case other => throw new IllegalArgumentException(s"Invalid Avro schema type:$other")
      }
    }.orNull
  }

}
