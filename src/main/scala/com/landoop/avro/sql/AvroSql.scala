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

import java.util

import com.landoop.avro.sql.AvroSchemaSql._
import com.landoop.sql.{Field, SqlContext}
import io.confluent.kafka.serializers.NonRecordContainer
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericContainer, GenericData, IndexedRecord}
import org.apache.avro.util.Utf8
import org.apache.calcite.sql.SqlSelect

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
import scala.util.{Failure, Success, Try}

object AvroSql extends AvroFieldValueGetter {
  private val StringSchema = Schema.create(Schema.Type.STRING)

  implicit class IndexedRecordExtension(val record: IndexedRecord) extends AnyVal {
    def get(fieldName: String): Any = {
      Option(record.getSchema.getField(fieldName))
        .map(f => record.get(f.pos))
        .orNull
    }
  }

  implicit class GenericContainerKcqlConverter(val from: GenericContainer) extends AnyVal {
    def sql(query: String): GenericContainer = {
      import org.apache.calcite.config.Lex
      import org.apache.calcite.sql.parser.SqlParser
      val config = SqlParser.configBuilder
        .setLex(Lex.MYSQL)
        .setCaseSensitive(false)
        .setIdentifierMaxLength(250)
        .build

      val withStructure: Boolean = query.trim.toLowerCase().endsWith("withstructure")
      val sql = if (withStructure) {
        query.trim.dropRight("withstructure".length)
      } else query

      val parser = SqlParser.create(sql, config)
      val select = Try(parser.parseQuery()) match {
        case Failure(e) => throw new IllegalArgumentException(s"Query is not valid.${e.getMessage}")
        case Success(sqlSelect: SqlSelect) => sqlSelect
        case Success(sqlNode) => throw new IllegalArgumentException("Only `select` statements are allowed")
      }
      this.sql(select, !withStructure)
    }

    def sql(query: SqlSelect, flatten: Boolean): GenericContainer = {
      Option(from).map { _ =>
        from match {
          case _: NonRecordContainer =>
          case _: IndexedRecord =>
          case other => throw new IllegalArgumentException(s"Avro type ${other.getClass.getName} is not supported")
        }
        if (!flatten) {
          implicit val sqlContext = new SqlContext(Field.from(query))
          val schema = from.getSchema.copy()
          sql(schema)
        } else {
          implicit val fields = Field.from(query)
          val schema = from.getSchema.flatten(fields)
          this.flatten(schema)
        }
      }
    }.orNull

    def sql(fields: Seq[Field], flatten: Boolean): GenericContainer = {
      Option(from).map { _ =>
        from match {
          case _: NonRecordContainer =>
          case _: IndexedRecord =>
          case other => throw new IllegalArgumentException(s"Avro type ${other.getClass.getName} is not supported")
        }
        if (!flatten) {
          implicit val sqlContext = new SqlContext(fields)
          val schema = from.getSchema.copy()
          sql(schema)
        } else {
          implicit val f = fields
          val schema = from.getSchema.flatten(fields)
          this.flatten(schema)
        }
      }.orNull
    }


    private[sql] def sql(newSchema: Schema)(implicit sqlContext: SqlContext): GenericContainer = {
      from match {
        case container: NonRecordContainer =>
          sqlContext.fields match {
            case Seq(f) if f.name == "*" => container
            case _ => throw new IllegalArgumentException(s"Can't select specific fields from primitive avro record:${from.getClass.getName}")
          }
        case record: IndexedRecord => fromRecord(record, record.getSchema, newSchema, Vector.empty[String]).asInstanceOf[IndexedRecord]
        case other => throw new IllegalArgumentException(s"${other.getClass.getName} is not handled")
      }
    }

    private[sql] def flatten(newSchema: Schema)(implicit fields: Seq[Field]): GenericContainer = {
      from match {
        case container: NonRecordContainer => flattenPrimitive(container)
        case record: IndexedRecord => flattenIndexedRecord(record, newSchema)
        case other => throw new IllegalArgumentException(s"${other.getClass.getName} is not handled")
      }
    }

    private def flattenPrimitive(value: NonRecordContainer)(implicit fields: Seq[Field]): GenericContainer = {
      fields match {
        case Seq(f) if f.name == "*" => value
        case _ => throw new IllegalArgumentException(s"Can't select multiple fields from ${value.getSchema}")
      }
    }

    private def flattenIndexedRecord(record: IndexedRecord, newSchema: Schema)(implicit fields: Seq[Field]): GenericContainer = {
      val fieldsParentMap = fields.foldLeft(Map.empty[String, ArrayBuffer[String]]) { case (map, f) =>
        val key = Option(f.parents).map(_.mkString(".")).getOrElse("")
        val buffer = map.getOrElse(key, ArrayBuffer.empty[String])
        buffer += f.name
        map + (key -> buffer)
      }

      val newRecord = new GenericData.Record(newSchema)
      fields.foldLeft(0) { case (index, field) =>
        if (field.name == "*") {
          val sourceFields = record.getSchema.getFields(Option(field.parents).getOrElse(Seq.empty))
          val key = Option(field.parents).map(_.mkString(".")).getOrElse("")
          sourceFields
            .filter { f =>
              fieldsParentMap.get(key).forall(!_.contains(f.name))
            }.foldLeft(index) { case (i, f) =>
            val extractedValue = get(record, record.getSchema, Option(field.parents).getOrElse(Seq.empty[String]) :+ f.name)
            newRecord.put(i, extractedValue.orNull)
            i + 1
          }
        }
        else {
          val extractedValue = get(record, record.getSchema, Option(field.parents).getOrElse(Seq.empty[String]) :+ field.name)
          newRecord.put(index, extractedValue.orNull)
          index + 1
        }
      }
      newRecord
    }

    private[sql] def fromUnion(value: Any,
                               fromSchema: Schema,
                               targetSchema: Schema,
                               parents: Seq[String])(implicit sqlContext: SqlContext): Any = {
      from(value, fromSchema.fromUnion(), targetSchema.fromUnion(), parents)
    }


    private[sql] def fromArray(value: Any,
                               schema: Schema,
                               targetSchema:
                               Schema,
                               parents: Seq[String])(implicit sqlContext: SqlContext): Any = {
      value match {
        case c: java.util.Collection[_] =>
          c.foldLeft(new java.util.ArrayList[Any](c.size())) { (acc, e) =>
            acc.add(from(e, schema.getElementType, targetSchema.getElementType, parents))
            acc
          }
        case other => throw new IllegalArgumentException(s"${other.getClass.getName} is not handled")
      }
    }

    private[sql] def fromRecord(value: Any,
                                schema: Schema,
                                targetSchema: Schema,
                                parents: Seq[String])(implicit sqlContext: SqlContext): Any = {
      val record = value.asInstanceOf[IndexedRecord]
      val fields = sqlContext.getFieldsForPath(parents)
      //.get(parents.head)
      val fieldsTuple = fields.headOption.map { _ =>
        fields.flatMap {
          case Left(field) if field.name == "*" =>
            val filteredFields = fields.collect { case Left(f) if f.name != "*" => f.name }.toSet

            schema.getFields
              .withFilter(f => !filteredFields.contains(f.name()))
              .map { f =>
                val sourceField = Option(schema.getField(f.name))
                  .getOrElse(throw new IllegalArgumentException(s"${f.name} was not found in $schema"))
                sourceField -> f
              }

          case Left(field) =>
            val sourceField = Option(schema.getField(field.name))
              .getOrElse(throw new IllegalArgumentException(s"${field.name} can't be found in $schema"))

            val targetField = Option(targetSchema.getField(field.alias))
              .getOrElse(throw new IllegalArgumentException(s"${field.alias} can't be found in $targetSchema"))

            List(sourceField -> targetField)

          case Right(field) =>
            val sourceField = Option(schema.getField(field))
              .getOrElse(throw new IllegalArgumentException(s"$field can't be found in $schema"))

            val targetField = Option(targetSchema.getField(field))
              .getOrElse(throw new IllegalArgumentException(s"$field can't be found in $targetSchema"))

            List(sourceField -> targetField)

        }
      }.getOrElse {
        targetSchema.getFields
          .map { f =>
            val sourceField = Option(schema.getField(f.name))
              .getOrElse(throw new IllegalArgumentException(s"Can't find the field ${f.name} in ${schema.getFields.map(_.name()).mkString(",")}"))
            sourceField -> f
          }
      }

      val newRecord = new GenericData.Record(targetSchema)
      fieldsTuple.foreach { case (sourceField, targetField) =>
        val v = from(record.get(sourceField.name()),
          sourceField.schema(),
          targetField.schema(),
          parents :+ sourceField.name)
        newRecord.put(targetField.name(), v)
      }
      newRecord
    }

    private[sql] def fromMap(value: Any, fromSchema: Schema,
                             targetSchema: Schema,
                             parents: Seq[String])(implicit sqlContext: SqlContext): Any = {
      Option(value.asInstanceOf[java.util.Map[CharSequence, Any]]).map { map =>
        val newMap = new util.HashMap[CharSequence, Any]()
        //check if there are keys for this
        val fields = sqlContext.getFieldsForPath(parents)
        val initialMap = {
          if (fields.exists(f => f.isLeft && f.left.get.name == "*")) {
            map.keySet().map(k => k.toString -> k.toString).toMap
          } else {
            Map.empty[String, String]
          }
        }

        fields.headOption.map { _ =>
          fields.filterNot(f => f.isLeft && f.left.get.name != "*")
            .foldLeft(initialMap) {
              case (m, Left(f)) => m + (f.name -> f.alias)
              case (m, Right(f)) => m + (f -> f)
            }
        }
          .getOrElse(map.keySet().map(k => k.toString -> k.toString).toMap)
          .foreach { case (key, alias) =>
            Option(map.get(key)).foreach { v =>
              newMap.put(
                from(key, StringSchema, StringSchema, null).asInstanceOf[CharSequence],
                from(v, fromSchema.getValueType, targetSchema.getValueType, parents))
            }
          }
        newMap
      }.orNull
    }

    private[sql] def from(from: Any,
                          fromSchema: Schema,
                          targetSchema: Schema,
                          parents: Seq[String])(implicit sqlContext: SqlContext): Any = {
      Option(from).map { _ =>
        implicit val s = fromSchema
        fromSchema.getType match {
          case Schema.Type.BOOLEAN | Schema.Type.NULL |
               Schema.Type.DOUBLE | Schema.Type.FLOAT |
               Schema.Type.LONG | Schema.Type.INT |
               Schema.Type.ENUM | Schema.Type.BYTES | Schema.Type.FIXED => from

          case Schema.Type.STRING => new Utf8(from.toString).asInstanceOf[Any] //yes UTF8

          case Schema.Type.UNION => fromUnion(from, fromSchema, targetSchema, parents)

          case Schema.Type.ARRAY => fromArray(from, fromSchema, targetSchema, parents)

          case Schema.Type.MAP => fromMap(from, fromSchema, targetSchema, parents)

          case Schema.Type.RECORD => fromRecord(from, fromSchema, targetSchema, parents)

          case other => throw new IllegalArgumentException(s"Invalid Avro schema type:$other")
        }
      }.orNull
    }
  }

}
