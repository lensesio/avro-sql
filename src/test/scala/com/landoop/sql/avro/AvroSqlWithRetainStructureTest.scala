package com.landoop.sql.avro

import com.landoop.avro.sql.AvroSql._
import com.sksamuel.avro4s.{RecordFormat, SchemaFor, ToRecord}
import io.confluent.kafka.serializers.NonRecordContainer
import org.apache.avro.Schema
import org.apache.avro.generic.GenericContainer
import org.scalatest.{Matchers, WordSpec}

class AvroSqlWithRetainStructureTest extends WordSpec with Matchers {

  private def compare[T](actual: GenericContainer, t: T)(implicit schemaFor: SchemaFor[T], toRecord: ToRecord[T]) = {
    val expectedSchema = schemaFor().toString()
      .replace("LocalPizza", "Pizza")
      .replace("LocalIngredient", "Ingredient")

    actual.getSchema.toString() shouldBe expectedSchema

    val expectedRecord = toRecord.apply(t)
    actual.toString shouldBe expectedRecord.toString
  }

  "AvroSql" should {
    "handle null payload" in {
      null.asInstanceOf[GenericContainer].sql("SELECT * FROM topic  withstructure") shouldBe null.asInstanceOf[Any]
    }

    "throw an exception when the parameter is not a GenericRecord or NonRecordContainer" in {
      intercept[IllegalArgumentException] {
        new GenericContainer {
          override def getSchema = null
        }.sql("SELECT * FROM topic  withstructure")
      }
    }

    "handle Int avro record" in {
      val container = new NonRecordContainer(Schema.create(Schema.Type.INT), 2000)

      val expected = new NonRecordContainer(Schema.create(Schema.Type.INT), 2000)
      container.sql("SELECT * FROM topic  withstructure") shouldBe expected
    }

    "handle Nullable Int avro record with a integer value" in {
      val nullSchema = Schema.create(Schema.Type.NULL)
      val intSchema = Schema.create(Schema.Type.INT)
      val schema = Schema.createUnion(nullSchema, intSchema)

      val container = new NonRecordContainer(schema, 2000)
      val expected = new NonRecordContainer(schema, 2000)

      container.sql("SELECT * FROM topic withstructure") shouldBe expected
    }

    "handle Nullable Int avro record with a null value" in {
      val nullSchema = Schema.create(Schema.Type.NULL)
      val intSchema = Schema.create(Schema.Type.INT)
      val schema = Schema.createUnion(nullSchema, intSchema)

      val container = new NonRecordContainer(schema, null)
      val expected = new NonRecordContainer(schema, null)
      container.sql("SELECT * FROM topic  withstructure") shouldBe expected
    }

    "throw an exception when trying to select field of an Int avro record" in {
      val expected = 2000
      val container = new NonRecordContainer(Schema.create(Schema.Type.INT), expected)
      intercept[IllegalArgumentException] {
        container.sql("SELECT field1 FROM topic  withstructure")
      }
    }

    "handle 'SELECT * FROM topic withstructure' for a record" in {
      val pepperoni = Pizza("pepperoni", Seq(Ingredient("pepperoni", 12, 4.4), Ingredient("onions", 1, 0.4)), false, false, 98)

      val schema = SchemaFor[Pizza]()
      val toRecord = ToRecord[Pizza]
      val record = RecordFormat[Pizza].to(pepperoni)

      val actual = record.sql("SELECT *FROM topic withstructure")
      actual shouldBe record
    }

    "handle 'SELECT *, name as fieldName FROM topic withstructure' for a record" in {
      val pepperoni = Pizza("pepperoni", Seq(Ingredient("pepperoni", 12, 4.4), Ingredient("onions", 1, 0.4)), false, false, 98)

      val schema = SchemaFor[Pizza]()
      val toRecord = ToRecord[Pizza]
      val record = RecordFormat[Pizza].to(pepperoni)

      val actual = record.sql("SELECT *, name as fieldName FROM topic withstructure")

      case class LocalIngredient(name: String, sugar: Double, fat: Double)
      case class LocalPizza(ingredients: Seq[LocalIngredient], vegetarian: Boolean, vegan: Boolean, calories: Int, fieldName: String)

      val newpepperoni = LocalPizza(Seq(LocalIngredient("pepperoni", 12, 4.4), LocalIngredient("onions", 1, 0.4)), false, false, 98, "pepperoni")
      compare(actual, newpepperoni)
    }

    "handle 'SELECT *, ingredients as stuff FROM topic withstructure' for a record" in {
      val pepperoni = Pizza("pepperoni", Seq(Ingredient("pepperoni", 12, 4.4), Ingredient("onions", 1, 0.4)), false, false, 98)

      val schema = SchemaFor[Pizza]()
      val toRecord = ToRecord[Pizza]
      val record = RecordFormat[Pizza].to(pepperoni)

      val actual = record.sql("SELECT *, ingredients as stuff FROM topic withstructure")

      case class LocalIngredient(name: String, sugar: Double, fat: Double)
      case class LocalPizza(name: String, vegetarian: Boolean, vegan: Boolean, calories: Int, stuff: Seq[LocalIngredient])

      val newpepperoni = LocalPizza(pepperoni.name, pepperoni.vegetarian, pepperoni.vegan, pepperoni.calories, Seq(LocalIngredient("pepperoni", 12, 4.4), LocalIngredient("onions", 1, 0.4)))
      compare(actual, newpepperoni)
    }

    "handle 'SELECT name as fieldName, * FROM topic withstructure' for a record" in {
      val pepperoni = Pizza("pepperoni", Seq(Ingredient("pepperoni", 12, 4.4), Ingredient("onions", 1, 0.4)), false, false, 98)

      val schema = SchemaFor[Pizza]()
      val toRecord = ToRecord[Pizza]
      val record = RecordFormat[Pizza].to(pepperoni)

      val actual = record.sql("SELECT name as fieldName, * FROM topic withstructure")

      case class LocalIngredient(name: String, sugar: Double, fat: Double)
      case class LocalPizza(fieldName: String, ingredients: Seq[LocalIngredient], vegetarian: Boolean, vegan: Boolean, calories: Int)
      val newpepperoni = LocalPizza(pepperoni.name, Seq(LocalIngredient("pepperoni", 12, 4.4), LocalIngredient("onions", 1, 0.4)), pepperoni.vegetarian, pepperoni.vegan, pepperoni.calories)
      compare(actual, newpepperoni)
    }

    "handle 'SELECT vegan FROM topic withstructure' for a record" in {
      val pepperoni = Pizza("pepperoni", Seq(Ingredient("pepperoni", 12, 4.4), Ingredient("onions", 1, 0.4)), false, false, 98)

      val schema = SchemaFor[Pizza]()
      val toRecord = ToRecord[Pizza]
      val record = RecordFormat[Pizza].to(pepperoni)

      val actual = record.sql("SELECT vegan FROM topic withstructure")

      case class LocalPizza(vegan: Boolean)
      val newpepperoni = LocalPizza(pepperoni.vegan)
      compare(actual, newpepperoni)
    }

    "handle 'SELECT vegan as veganA FROM topic withstructure' for a record" in {
      val pepperoni = Pizza("pepperoni", Seq(Ingredient("pepperoni", 12, 4.4), Ingredient("onions", 1, 0.4)), false, false, 98)

      val schema = SchemaFor[Pizza]()
      val toRecord = ToRecord[Pizza]
      val record = RecordFormat[Pizza].to(pepperoni)

      val actual = record.sql("SELECT vegan as veganA FROM topic withstructure")

      case class LocalPizza(veganA: Boolean)
      val newpepperoni = LocalPizza(pepperoni.vegan)
      compare(actual, newpepperoni)
    }

    "handle 'SELECT ingredients.name FROM topic withstructure' for a record" in {
      val pepperoni = Pizza("pepperoni", Seq(Ingredient("pepperoni", 12, 4.4), Ingredient("onions", 1, 0.4)), false, false, 98)

      val schema = SchemaFor[Pizza]()
      val toRecord = ToRecord[Pizza]
      val record = RecordFormat[Pizza].to(pepperoni)

      val actual = record.sql("SELECT ingredients.name FROM topic withstructure")

      case class LocalIngredient(name: String)
      case class LocalPizza(ingredients: Seq[LocalIngredient])
      val newpepperoni = LocalPizza(Seq(LocalIngredient("pepperoni"), LocalIngredient("onions")))
      compare(actual, newpepperoni)
    }

    "handle 'SELECT ingredients.name, ingredients.sugar FROM topic withstructure' for a record" in {
      val pepperoni = Pizza("pepperoni", Seq(Ingredient("pepperoni", 12, 4.4), Ingredient("onions", 1, 0.4)), false, false, 98)

      val schema = SchemaFor[Pizza]()
      val toRecord = ToRecord[Pizza]
      val record = RecordFormat[Pizza].to(pepperoni)

      val actual = record.sql("SELECT ingredients.name, ingredients.sugar FROM topic withstructure")

      case class LocalIngredient(name: String, sugar: Double)
      case class LocalPizza(ingredients: Seq[LocalIngredient])
      val newpepperoni = LocalPizza(Seq(LocalIngredient("pepperoni", 12), LocalIngredient("onions", 1)))
      compare(actual, newpepperoni)
    }

    "handle 'SELECT ingredients.name as fieldName, ingredients.sugar as fieldSugar FROM topic withstructure' for a record" in {
      val pepperoni = Pizza("pepperoni", Seq(Ingredient("pepperoni", 12, 4.4), Ingredient("onions", 1, 0.4)), false, false, 98)

      val schema = SchemaFor[Pizza]()
      val toRecord = ToRecord[Pizza]
      val record = RecordFormat[Pizza].to(pepperoni)

      val actual = record.sql("SELECT ingredients.name as fieldName, ingredients.sugar as fieldSugar FROM topic withstructure")

      case class LocalIngredient(fieldName: String, fieldSugar: Double)
      case class LocalPizza(ingredients: Seq[LocalIngredient])
      val newpepperoni = LocalPizza(Seq(LocalIngredient("pepperoni", 12), LocalIngredient("onions", 1)))
      compare(actual, newpepperoni)
    }


    "handle 'SELECT ingredients.*,ingredients.name as fieldName, ingredients.sugar as fieldSugar FROM topic withstructure' for a record" in {
      val pepperoni = Pizza("pepperoni", Seq(Ingredient("pepperoni", 12, 4.4), Ingredient("onions", 1, 0.4)), false, false, 98)

      val schema = SchemaFor[Pizza]()
      val toRecord = ToRecord[Pizza]
      val record = RecordFormat[Pizza].to(pepperoni)

      val actual = record.sql("SELECT ingredients.*,ingredients.name as fieldName, ingredients.sugar as fieldSugar FROM topic withstructure")
      case class LocalIngredient(fat: Double, fieldName: String, fieldSugar: Double)
      case class LocalPizza(ingredients: Seq[LocalIngredient])
      val newpepperoni = LocalPizza(Seq(LocalIngredient(4.4, "pepperoni", 12), LocalIngredient(0.4, "onions", 1)))
      compare(actual, newpepperoni)
    }

    "handle 'SELECT ingredients.name as fieldName,ingredients.*, ingredients.sugar as fieldSugar FROM topic withstructure' for a record" in {
      val pepperoni = Pizza("pepperoni", Seq(Ingredient("pepperoni", 12, 4.4), Ingredient("onions", 1, 0.4)), false, false, 98)

      val schema = SchemaFor[Pizza]()
      val toRecord = ToRecord[Pizza]
      val record = RecordFormat[Pizza].to(pepperoni)

      val actual = record.sql("SELECT ingredients.name as fieldName,ingredients.*, ingredients.sugar as fieldSugar FROM topic withstructure")

      case class LocalIngredient(fieldName: String, fat: Double, fieldSugar: Double)
      case class LocalPizza(ingredients: Seq[LocalIngredient])
      val newpepperoni = LocalPizza(Seq(LocalIngredient("pepperoni", 4.4, 12), LocalIngredient("onions", 0.4, 1)))
      compare(actual, newpepperoni)
    }

    "handle 'SELECT ingredients.name as fieldName, ingredients.sugar as fieldSugar, ingredients.* FROM topic withstructure' for a record" in {
      val pepperoni = Pizza("pepperoni", Seq(Ingredient("pepperoni", 12, 4.4), Ingredient("onions", 1, 0.4)), false, false, 98)

      val schema = SchemaFor[Pizza]()
      val toRecord = ToRecord[Pizza]
      val record = RecordFormat[Pizza].to(pepperoni)

      val actual = record.sql("SELECT ingredients.name as fieldName, ingredients.sugar as fieldSugar, ingredients.* FROM topic withstructure")

      case class LocalIngredient(fieldName: String, fieldSugar: Double, fat: Double)
      case class LocalPizza(ingredients: Seq[LocalIngredient])
      val newpepperoni = LocalPizza(Seq(LocalIngredient("pepperoni", 12, 4.4), LocalIngredient("onions", 1, 0.4)))
      compare(actual, newpepperoni)
    }


    "handle 'SELECT name, ingredients.name as fieldName, ingredients.sugar as fieldSugar, ingredients.* FROM topic withstructure' for a record" in {
      val pepperoni = Pizza("pepperoni", Seq(Ingredient("pepperoni", 12, 4.4), Ingredient("onions", 1, 0.4)), false, false, 98)

      val schema = SchemaFor[Pizza]()
      val toRecord = ToRecord[Pizza]
      val record = RecordFormat[Pizza].to(pepperoni)

      val actual = record.sql("SELECT name, ingredients.name as fieldName, ingredients.sugar as fieldSugar, ingredients.* FROM topic withstructure")

      case class LocalIngredient(fieldName: String, fieldSugar: Double, fat: Double)
      case class LocalPizza(name: String, ingredients: Seq[LocalIngredient])
      val newpepperoni = LocalPizza("pepperoni", Seq(LocalIngredient("pepperoni", 12, 4.4), LocalIngredient("onions", 1, 0.4)))
      compare(actual, newpepperoni)
    }

    "handle 'SELECT name, ingredients.name as fieldName, ingredients.sugar as fieldSugar, ingredients.*, calories as cals FROM topic withstructure' for a record" in {
      val pepperoni = Pizza("pepperoni", Seq(Ingredient("pepperoni", 12, 4.4), Ingredient("onions", 1, 0.4)), false, false, 98)

      val schema = SchemaFor[Pizza]()
      val toRecord = ToRecord[Pizza]
      val record = RecordFormat[Pizza].to(pepperoni)

      val actual = record.sql("SELECT name, ingredients.name as fieldName, ingredients.sugar as fieldSugar, ingredients.*, calories as cals FROM topic withstructure")
      case class LocalIngredient(fieldName: String, fieldSugar: Double, fat: Double)
      case class LocalPizza(name: String, ingredients: Seq[LocalIngredient], cals: Int)
      val newpepperoni = LocalPizza("pepperoni", Seq(LocalIngredient("pepperoni", 12, 4.4), LocalIngredient("onions", 1, 0.4)), 98)
      compare(actual, newpepperoni)
    }

    "handle 'SELECT name, ingredients.name as fieldName, calories as cals,ingredients.sugar as fieldSugar, ingredients.* FROM topic withstructure' for a record" in {
      val pepperoni = Pizza("pepperoni", Seq(Ingredient("pepperoni", 12, 4.4), Ingredient("onions", 1, 0.4)), false, false, 98)

      val schema = SchemaFor[Pizza]()
      val toRecord = ToRecord[Pizza]
      val record = RecordFormat[Pizza].to(pepperoni)

      val actual = record.sql("SELECT name, ingredients.name as fieldName, calories as cals, ingredients.sugar as fieldSugar, ingredients.* FROM topic withstructure")
      case class LocalIngredient(fieldName: String, fieldSugar: Double, fat: Double)
      case class LocalPizza(name: String, ingredients: Seq[LocalIngredient], cals: Int)
      val newpepperoni = LocalPizza("pepperoni", Seq(LocalIngredient("pepperoni", 12, 4.4), LocalIngredient("onions", 1, 0.4)), 98)
      compare(actual, newpepperoni)
    }
  }
}
