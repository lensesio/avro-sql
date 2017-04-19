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

import com.landoop.sql.{Field, SqlContext}
import org.apache.avro.Schema
import org.apache.avro.Schema.{Field => AvroField}
import org.apache.calcite.sql.SqlSelect

import scala.annotation.tailrec
import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

object AvroSchemaSql {

  implicit class AvroSchemaSqlExtension(val schema: Schema) extends AnyVal {

    def isNullable(): Boolean = {
      schema.getType == Schema.Type.UNION &&
        schema.getTypes.exists(_.getType == Schema.Type.NULL)
    }

    /**
      * This assumes a null, type union. probably better to look at the value and work out the schema
      */
    def fromUnion(): Schema = {
      schema.getTypes.toList match {
        case actualSchema +: Nil => actualSchema
        case List(n, actualSchema) if n.getType == Schema.Type.NULL => actualSchema
        case List(actualSchema, n) if n.getType == Schema.Type.NULL => actualSchema
        case _ => throw new IllegalArgumentException("Unions has one specific type and null")
      }
    }

    def getFields(path: Seq[String]): Seq[AvroField] = {
      def navigate(current: Schema, parents: Seq[String]): Seq[AvroField] = {
        if (Option(parents).isEmpty || parents.isEmpty) {
          current.getType match {
            case Schema.Type.RECORD => current.getFields
            case Schema.Type.UNION => navigate(current.fromUnion(), parents)
            case Schema.Type.MAP => throw new IllegalArgumentException(s"Can't select fields ${path.mkString(".")} since it resolved to a Map($current)")
            case _ => throw new IllegalArgumentException(s"Can't select fields ${path.mkString(".")} from schema:$current ")
          }
        } else {
          current.getType match {
            case Schema.Type.RECORD =>
              val field = Option(current.getField(parents.head))
                .getOrElse(throw new IllegalArgumentException(s"Can't find field ${parents.head} in schema:$current"))
              navigate(field.schema(), parents.tail)
            case Schema.Type.UNION => navigate(schema.fromUnion(), parents)
            case _ => throw new IllegalArgumentException(s"Can't select fields ${path.mkString(".")} from schema:$current ")
          }
        }
      }

      navigate(schema, path)
    }

    def fromPath(path: Seq[String]): Seq[AvroField] = {
      AvroSchemaExtension.fromPath(schema, path)
    }

    def copy(sql: SqlSelect, flatten: Boolean): Schema = {
      if (!flatten) {
        implicit val sqlContext = new SqlContext(Field.from(sql))
        copy
      }
      else {
        this.flatten(Field.from(sql))
      }
    }

    def copy()(implicit sqlContext: SqlContext): Schema = {
      AvroSchemaExtension.copy(schema, Vector.empty)
    }

    def copyAsNullable(): Schema = {
      schema.getType match {
        case Schema.Type.UNION =>
          if (schema.getTypes.get(0).getType == Schema.Type.NULL) {
            schema
          }
          else {
            val newSchema = Schema.createUnion(Schema.create(Schema.Type.NULL) +: schema.getTypes)
            newSchema.copyProperties(schema)
          }
        case _ => Schema.createUnion(Schema.create(Schema.Type.NULL), schema)
      }
    }

    def flatten(fields: Seq[Field]): Schema = {
      def allowOnlyStarSelection() = {
        fields match {
          case Seq(f) if f.name == "*" => schema
          case _ => throw new IllegalArgumentException(s"You can't select fields from schema:$schema")
        }
      }

      schema.getType match {
        case Schema.Type.ARRAY | Schema.Type.MAP => throw new IllegalArgumentException(s"Can't flattent schema type:${schema.getType}")
        case Schema.Type.BOOLEAN | Schema.Type.BYTES |
             Schema.Type.DOUBLE | Schema.Type.ENUM |
             Schema.Type.FIXED | Schema.Type.FLOAT |
             Schema.Type.INT | Schema.Type.LONG |
             Schema.Type.NULL | Schema.Type.STRING => allowOnlyStarSelection()

        //case Schema.Type.MAP => allowOnlyStarSelection()
        case Schema.Type.UNION => schema.fromUnion().flatten(fields)
        case Schema.Type.RECORD =>
          fields match {
            case Seq(f) if f.name == "*" => schema
            case _ => createRecordSchemaForFlatten(fields)
          }
      }
    }

    private[sql] def copyProperties(from: Schema): Schema = {
      from.getType match {
        case Schema.Type.RECORD | Schema.Type.FIXED | Schema.Type.ENUM =>
          from.getAliases.foreach(schema.addAlias)
        case _ =>
      }
      from.getObjectProps.foreach { case (prop: String, value: Any) =>
        schema.addProp(prop, value)
      }
      schema
    }

    private def createRecordSchemaForFlatten(fields: Seq[Field]): Schema = {
      val newSchema = Schema.createRecord(schema.getName, schema.getDoc, schema.getNamespace, false)
      val fieldParentsMap = fields.foldLeft(Map.empty[String, ArrayBuffer[String]]) { case (map, f) =>
        val key = Option(f.parents).map(_.mkString(".")).getOrElse("")
        val buffer = map.getOrElse(key, ArrayBuffer.empty[String])
        if (buffer.contains(f.name)) {
          throw new IllegalArgumentException(s"You have defined the field ${
            if (f.hasParents) {
              f.parents.mkString(".") + "." + f.name
            } else {
              f.name
            }
          } more than once!")
        }
        buffer += f.name
        map + (key -> buffer)
      }

      val colsMap = collection.mutable.Map.empty[String, Int]

      def getNextFieldName(fieldName: String): String = {
        colsMap.get(fieldName).map { v =>
          colsMap.put(fieldName, v + 1)
          s"${fieldName}_${v + 1}"
        }.getOrElse {
          colsMap.put(fieldName, 0)
          fieldName
        }
      }

      val newFields = fields.flatMap {

        case field if field.name == "*" =>
          val siblings = fieldParentsMap.get(Option(field.parents).map(_.mkString(".")).getOrElse(""))
          Option(field.parents)
            .map { p =>
              val s = schema.fromPath(p)
                .headOption
                .getOrElse(throw new IllegalArgumentException(s"Can't find field ${p.mkString(".")} in schema:$schema"))
                .schema()

              s.getType match {
                case Schema.Type.UNION =>
                  val underlyingSchema = s.fromUnion()
                  underlyingSchema.getType match {
                    case Schema.Type.RECORD =>
                      if (!underlyingSchema.isNullable()) underlyingSchema.getFields.toSeq
                      else underlyingSchema.getFields.map { f =>
                        new AvroField(f.name(), f.schema().copyAsNullable, f.doc(), f.defaultVal())
                      }
                    case other => throw new IllegalArgumentException(s"Field selection ${p.mkString(".")} resolves to schema type:$other. Only RECORD type is allowed")
                  }
                case Schema.Type.RECORD =>
                  if (!s.isNullable()) s.getFields.toSeq
                  else s.getFields.map { f =>
                    new AvroField(f.name(), f.schema().copyAsNullable, f.doc(), f.defaultVal())
                  }
                case other =>
                  throw new IllegalArgumentException(s"Field selection ${p.mkString(".")} resolves to schema type:$other. Only RECORD type is allowed")
              }
            }
            .getOrElse {
              if (!schema.isNullable) schema.getFields.toSeq
              else schema.getFields.map { f =>
                new AvroField(f.name(), f.schema().copyAsNullable, f.doc(), f.defaultVal())
              }
            }
            .withFilter { f =>
              siblings.collect { case s if s.contains(f.name()) => false }.getOrElse(true)
            }
            .map { f =>
              AvroSchemaExtension.checkAllowedSchemas(f.schema(), field)
              new AvroField(getNextFieldName(f.name()), f.schema(), f.doc(), f.defaultVal())
            }

        case field if field.hasParents =>
          schema.fromPath(field.parents :+ field.name)
            .map { extracted =>
              require(extracted != null, s"Invalid field:${(field.parents :+ field.name).mkString(".")}")
              AvroSchemaExtension.checkAllowedSchemas(extracted.schema(), field)
              if (field.alias == "*") {
                new AvroField(getNextFieldName(extracted.name()), extracted.schema(), extracted.doc(), extracted.defaultVal())
              } else {
                new AvroField(getNextFieldName(field.alias), extracted.schema(), extracted.doc(), extracted.defaultVal())
              }
            }

        case field =>
          val originalField = Option(schema.getField(field.name))
            .getOrElse(throw new IllegalArgumentException(s"Can't find field:${field.name} in schema:$schema"))
          AvroSchemaExtension.checkAllowedSchemas(originalField.schema(), field)
          Seq(new AvroField(getNextFieldName(field.alias), originalField.schema(), originalField.doc(), originalField.defaultVal()))
      }


      newSchema.setFields(newFields)
      newSchema.copyProperties(schema)
    }
  }

  private object AvroSchemaExtension {
    def copy(from: Schema, parents: Vector[String])(implicit sqlContext: SqlContext): Schema = {
      from.getType match {
        case Schema.Type.RECORD => createRecordSchema(from, parents)
        case Schema.Type.ARRAY =>
          val newSchema = Schema.createArray(copy(from.getElementType, parents))
          newSchema.copyProperties(from)
        case Schema.Type.MAP =>
          val elementSchema = copy(from.getValueType, parents)
          val newSchema = Schema.createMap(elementSchema)
          newSchema.copyProperties(from)

        case Schema.Type.UNION =>
          val newSchema = Schema.createUnion(from.getTypes.map(copy(_, parents)))
          newSchema.copyProperties(from)

        case _ => from
      }
    }


    private def createRecordSchema(from: Schema, parents: Vector[String])(implicit sqlContext: SqlContext): Schema = {
      val newSchema = Schema.createRecord(from.getName, from.getDoc, from.getNamespace, false)

      val fields = sqlContext.getFieldsForPath(parents)
      val newFields: Seq[Schema.Field] = fields match {
        case Seq() =>
          from.getFields
            .map { schemaField =>
              val newSchema = copy(schemaField.schema(), parents :+ schemaField.name)
              val newField = new org.apache.avro.Schema.Field(schemaField.name, newSchema, schemaField.doc(), schemaField.defaultVal())
              schemaField.aliases().foreach(newField.addAlias)
              newField
            }

        case Seq(Left(f)) if f.name == "*" =>
          from.getFields.map { schemaField =>
            val newSchema = copy(schemaField.schema(), parents :+ schemaField.name)
            val newField = new org.apache.avro.Schema.Field(schemaField.name, newSchema, schemaField.doc(), schemaField.defaultVal())
            schemaField.aliases().foreach(newField.addAlias)
            newField
          }
        case other =>
          fields.flatMap {
            case Left(field) if field.name == "*" =>
              from.getFields
                .withFilter(f => !fields.exists(e => e.isLeft && e.left.get.name == f.name))
                .map { f =>
                  val newSchema = copy(f.schema(), parents :+ f.name)
                  newSchema.copyProperties(f.schema())
                  val newField = new org.apache.avro.Schema.Field(f.name(), newSchema, f.doc(), f.defaultVal())
                  newField
                }.toList

            case Left(field) =>
              val originalField = Option(from.getField(field.name)).getOrElse(
                throw new IllegalArgumentException(s"Invalid selecting ${parents.mkString("", ".", ".")}${field.name}. Schema doesn't contain it."))
              val newSchema = copy(originalField.schema(), parents :+ field.name)
              newSchema.copyProperties(originalField.schema())
              val newField = new org.apache.avro.Schema.Field(field.alias, newSchema, originalField.doc(), originalField.defaultVal())
              Seq(newField)

            case Right(field) =>
              val originalField = Option(from.getField(field))
                .getOrElse(throw new IllegalArgumentException(s"Invalid selecting ${parents.mkString("", ".", ".")}$field. Schema doesn't contain it."))
              val newSchema = copy(originalField.schema(), parents :+ field)
              newSchema.copyProperties(originalField.schema())
              val newField = new org.apache.avro.Schema.Field(field, newSchema, originalField.doc(), originalField.defaultVal())
              Seq(newField)
          }
      }

      newSchema.setFields(newFields)
      newSchema.copyProperties(from)
    }

    def fromPath(from: Schema, path: Seq[String]): Seq[AvroField] = {
      fromPathInternal(from: Schema, path, from.isNullable())
    }

    @tailrec
    private def fromPathInternal(from: Schema, path: Seq[String], isOptional: Boolean): Seq[AvroField] = {
      path match {
        case Seq(field) if field == "*" =>
          from.getType match {
            case Schema.Type.RECORD =>
              if (!isOptional) from.getFields.toSeq
              else from.getFields.map(asNullable)

            case Schema.Type.UNION =>
              val underlyingSchema = from.fromUnion()
              underlyingSchema.getType match {
                case Schema.Type.RECORD =>
                  if (!isOptional) underlyingSchema.getFields.toSeq
                  else underlyingSchema.getFields.map(asNullable)

                case other => throw new IllegalArgumentException(s"Can't select field:$field from ${other.toString}")
              }
            case other => throw new IllegalArgumentException(s"Can't select field:$field from ${other.toString}")
          }
        case Seq(field) =>
          from.getType match {
            case Schema.Type.RECORD =>
              if (!isOptional) Seq(from.getField(field))
              else Seq(asNullable(from.getField(field)))

            case Schema.Type.UNION =>
              val underlyingSchema = from.fromUnion()
              underlyingSchema.getType match {
                case Schema.Type.RECORD =>
                  if (!isOptional) underlyingSchema.getFields.toSeq
                  else underlyingSchema.getFields.map(asNullable)

                case other => throw new IllegalArgumentException(s"Can't select field:$field from ${other.toString}")
              }

            case other => throw new IllegalArgumentException(s"Can't select field:$field from ${other.toString}")
          }
        case head +: tail =>
          val next = Option(from.getField(head))
            .getOrElse(throw new IllegalArgumentException(s"Can't find the field '$head'"))
          fromPathInternal(next.schema(), tail, isOptional || next.schema().isNullable())
      }
    }

    private def asNullable(f: AvroField): AvroField = {
      new AvroField(f.name(), f.schema().copyAsNullable(), f.doc(), f.defaultVal())
    }

    @tailrec
    def checkAllowedSchemas(schema: Schema, field: Field): Unit = {
      schema.getType match {
        case Schema.Type.ARRAY | Schema.Type.MAP => throw new IllegalArgumentException(s"Can't flatten from schema:$schema by selecting '${field.name}'")
        case Schema.Type.UNION => checkAllowedSchemas(schema.fromUnion(), field)
        case _ =>
      }
    }
  }

}
