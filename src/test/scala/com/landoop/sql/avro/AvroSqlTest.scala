package com.landoop.sql.avro

import com.landoop.avro.sql.AvroSql._
import com.sksamuel.avro4s.{RecordFormat, SchemaFor, ToRecord}
import io.confluent.kafka.serializers.NonRecordContainer
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericContainer, GenericRecord}
import org.scalatest.{Matchers, WordSpec}

class AvroSqlTest extends WordSpec with Matchers {

  private def compare[T](actual: GenericContainer, t: T)(implicit schemaFor: SchemaFor[T], toRecord: ToRecord[T]) = {
    val expectedSchema = schemaFor().toString()
      .replace("LocalPerson", "Person")
      .replace("LocalAddress", "Address")
      .replace("LocalStreet", "Street")
      .replace("LocalPizza", "Pizza")
      .replace("LocalSimpleAddress", "SimpleAddress")

    actual.getSchema.toString() shouldBe expectedSchema

    val expectedRecord = toRecord.apply(t)
    actual.toString shouldBe expectedRecord.toString
  }

  "AvroSql" should {
    "handle null payload" in {
      null.asInstanceOf[GenericContainer].sql("SELECT *") shouldBe null.asInstanceOf[Any]
    }

    "throw an exception when the parameter is not a GenericRecord or NonRecordContainer" in {
      intercept[IllegalArgumentException] {
        new GenericContainer {
          override def getSchema = null
        }.sql("SELECT *")
      }
    }

    "handle Int avro record" in {
      val container = new NonRecordContainer(Schema.create(Schema.Type.INT), 2000)

      val expected = new NonRecordContainer(Schema.create(Schema.Type.INT), 2000)
      container.sql("SELECT *") shouldBe expected
    }

    "handle Nullable Int avro record with a integer value" in {
      val nullSchema = Schema.create(Schema.Type.NULL)
      val intSchema = Schema.create(Schema.Type.INT)
      val schema = Schema.createUnion(nullSchema, intSchema)

      val container = new NonRecordContainer(schema, 2000)
      val expected = new NonRecordContainer(schema, 2000)

      container.sql("SELECT *") shouldBe expected
    }

    "handle Nullable Int avro record with a null value" in {
      val nullSchema = Schema.create(Schema.Type.NULL)
      val intSchema = Schema.create(Schema.Type.INT)
      val schema = Schema.createUnion(nullSchema, intSchema)

      val container = new NonRecordContainer(schema, null)
      val expected = new NonRecordContainer(schema, null)
      container.sql("SELECT *") shouldBe expected
    }

    "throw an exception when trying to select field of an Int avro record" in {
      val expected = 2000
      val container = new NonRecordContainer(Schema.create(Schema.Type.INT), expected)
      intercept[IllegalArgumentException] {
        container.sql("SELECT field1")
      }
    }

    "handle 'SELECT name,vegan, calories ' for a record" in {
      val pepperoni = Pizza("pepperoni", Seq(Ingredient("pepperoni", 12, 4.4), Ingredient("onions", 1, 0.4)), false, false, 98)

      val schema = SchemaFor[Pizza]()
      val toRecord = ToRecord[Pizza]
      val record = RecordFormat[Pizza].to(pepperoni)

      val actual = record.sql("SELECT name,vegan, calories")

      case class LocalPizza(name: String, vegan: Boolean, calories: Int)
      val expected = LocalPizza(pepperoni.name, pepperoni.vegan, pepperoni.calories)

      compare(actual, expected)
    }

    "handle 'SELECT name as fieldName,vegan as V, calories as C' for a record" in {
      val pepperoni = Pizza("pepperoni", Seq(Ingredient("pepperoni", 12, 4.4), Ingredient("onions", 1, 0.4)), false, false, 98)

      val schema = SchemaFor[Pizza]()
      val toRecord = ToRecord[Pizza]
      val record = RecordFormat[Pizza].to(pepperoni)

      val actual = record.sql("SELECT name as fieldName,vegan as V, calories as C")

      case class LocalPizza(fieldName: String, V: Boolean, C: Int)
      val expected = LocalPizza(pepperoni.name, pepperoni.vegan, pepperoni.calories)

      compare(actual, expected)
    }

    "handle 'SELECT calories as C ,vegan as V ,name as fieldName' for a record" in {
      val pepperoni = Pizza("pepperoni", Seq(Ingredient("pepperoni", 12, 4.4), Ingredient("onions", 1, 0.4)), false, false, 98)

      val schema = SchemaFor[Pizza]()
      val toRecord = ToRecord[Pizza]
      val record = RecordFormat[Pizza].to(pepperoni)

      val actual = record.sql("SELECT  calories as C,vegan as V,name as fieldName")

      case class LocalPizza(C: Int, V: Boolean, fieldName: String)
      val expected = LocalPizza(pepperoni.calories, pepperoni.vegan, pepperoni.name)

      compare(actual, expected)
    }

    "throw an exception when selecting arrays ' for a record" in {
      val pepperoni = Pizza("pepperoni", Seq(Ingredient("pepperoni", 12, 4.4), Ingredient("onions", 1, 0.4)), false, false, 98)

      val schema = SchemaFor[Pizza]()
      val toRecord = ToRecord[Pizza]
      val record = RecordFormat[Pizza].to(pepperoni)

      intercept[IllegalArgumentException] {
        record.sql("SELECT *, name as fieldName")
      }
    }

    "handle 'SELECT name, address.street.name' from record" in {
      val person = Person("Rick", Address(Street("Rock St"), None, "MtV", "CA", "94041", "USA"))

      val schema = SchemaFor[Person]()
      val toRecord = ToRecord[Person]
      val record = RecordFormat[Person].to(person)

      val actual = record.sql("SELECT name, address.street.name")

      case class LocalPerson(name: String, name_1: String)
      val localPerson = LocalPerson(person.name, person.address.street.name)
      compare(actual, localPerson)
    }

    "handle 'SELECT name, address.street.name as streetName' from record" in {
      val person = Person("Rick", Address(Street("Rock St"), None, "MtV", "CA", "94041", "USA"))

      val schema = SchemaFor[Person]()
      val toRecord = ToRecord[Person]
      val record: GenericRecord = RecordFormat[Person].to(person)

      val actual = record.sql("SELECT name, address.street.name as streetName")

      case class LocalPerson(name: String, streetName: String)
      val localPerson = LocalPerson(person.name, person.address.street.name)
      compare(actual, localPerson)
    }

    "handle 'SELECT name, address.street.name as streetName, address.street2.name as streetName2' from record" in {
      val person = Person("Rick", Address(Street("Rock St"), None, "MtV", "CA", "94041", "USA"))

      val schema = SchemaFor[Person]()
      val toRecord = ToRecord[Person]
      val record = RecordFormat[Person].to(person)

      val actual = record.sql("SELECT name, address.street.name as streetName, address.street2.name as streetName2")

      case class LocalPerson(name: String, streetName: String, streetName2: Option[String])
      val localPerson = LocalPerson(person.name, person.address.street.name, person.address.street2.map(_.name))
      compare(actual, localPerson)
    }

    "handle 'SELECT name, address.street.*, address.street2.name as streetName2' from record" in {
      val person = Person("Rick", Address(Street("Rock St"), None, "MtV", "CA", "94041", "USA"))

      val schema = SchemaFor[Person]()
      val toRecord = ToRecord[Person]
      val record = RecordFormat[Person].to(person)

      val actual = record.sql("SELECT name, address.street.*, address.street2.name as streetName2")

      case class LocalPerson(name: String, name_1: String, streetName2: Option[String])
      val localPerson = LocalPerson(person.name, person.address.street.name, person.address.street2.map(_.name))
      compare(actual, localPerson)
    }

    "handle 'SELECT name, address.street.*, address.street2.*' from record" in {
      val person = Person("Rick", Address(Street("Rock St"), None, "MtV", "CA", "94041", "USA"))

      val schema = SchemaFor[Person]()
      val toRecord = ToRecord[Person]
      val record = RecordFormat[Person].to(person)

      val actual = record.sql("SELECT name, address.street.*, address.street2.*")

      case class LocalPerson(name: String, name_1: String, name_2: String)
      val localPerson = LocalPerson(person.name, person.address.street.name, person.address.street2.map(_.name).orNull)
      compare(actual, localPerson)

      val person1 = Person("Rick", Address(Street("Rock St"), Some(Street("412 East")), "MtV", "CA", "94041", "USA"))
      val record1 = RecordFormat[Person].to(person1)

      val actual1 = record.sql("SELECT name, address.street.*, address.street2.*")
      val localPerson1 = LocalPerson(person.name, person.address.street.name, person.address.street2.map(_.name).orNull)
      compare(actual1, localPerson1)

    }

    "handle 'SELECT address.state, address.city,name, address.street.name' from record" in {
      val person = Person("Rick", Address(Street("Rock St"), None, "MtV", "CA", "94041", "USA"))

      val schema = SchemaFor[Person]()
      val toRecord = ToRecord[Person]
      val record = RecordFormat[Person].to(person)

      val actual = record.sql("SELECT address.state, address.city,name, address.street.name")

      case class LocalPerson(state: String, city: String, name: String, name_1: String)
      val localPerson = LocalPerson(person.address.state, person.address.city, person.name, person.address.street.name)
      compare(actual, localPerson)
    }

    "handle 'SELECT address.state as S, address.city as C,name, address.street.name' from record" in {
      val person = Person("Rick", Address(Street("Rock St"), None, "MtV", "CA", "94041", "USA"))

      val schema = SchemaFor[Person]()
      val toRecord = ToRecord[Person]
      val record = RecordFormat[Person].to(person)

      val actual = record.sql("SELECT address.state as S, address.city as C,name, address.street.name")

      case class LocalPerson(S: String, C: String, name: String, name_1: String)
      val localPerson = LocalPerson(person.address.state, person.address.city, person.name, person.address.street.name)
      compare(actual, localPerson)
    }

    "throw an exception if the field doesn't exist in the schema" in {
      val person = Person("Rick", Address(Street("Rock St"), None, "MtV", "CA", "94041", "USA"))

      val schema = SchemaFor[Person]()
      val toRecord = ToRecord[Person]
      val record = RecordFormat[Person].to(person)

      intercept[IllegalArgumentException] {
        record.sql("SELECT address.bam, address.city,name, address.street.name")
      }
    }


    "handle 'SELECT * FROM simpleAddress' from record" in {
      val address = SimpleAddress("Rock St", "MtV", "CA", "94041", "USA")

      val schema = SchemaFor[SimpleAddress]()
      val toRecord = ToRecord[SimpleAddress]
      val record = RecordFormat[SimpleAddress].to(address)

      val actual = record.sql("SELECT * FROM simpleAddress")
      actual shouldBe record
    }

    "handle 'SELECT street as S, city, state, zip as Z, country as C  FROM simpleAddress' from record" in {
      val address = SimpleAddress("Rock St", "MtV", "CA", "94041", "USA")

      val schema = SchemaFor[SimpleAddress]()
      val toRecord = ToRecord[SimpleAddress]
      val record = RecordFormat[SimpleAddress].to(address)

      val actual = record.sql("SELECT street as S, city, state, zip as Z, country as C  FROM simpleAddress")

      case class LocalSimpleAddress(S: String, city: String, state: String, Z: String, C: String)
      val expected = LocalSimpleAddress(address.street, address.city, address.state, address.zip, address.country)

      compare(actual, expected)
    }

    "handle 'SELECT  zip as Z, * FROM simpleAddress' from record" in {
      val address = SimpleAddress("Rock St", "MtV", "CA", "94041", "USA")

      val schema = SchemaFor[SimpleAddress]()
      val toRecord = ToRecord[SimpleAddress]
      val record = RecordFormat[SimpleAddress].to(address)

      val actual = record.sql("SELECT zip as Z, * FROM simpleAddress")

      case class LocalSimpleAddress(Z: String, street: String, city: String, state: String, country: String)
      val expected = LocalSimpleAddress(address.zip, address.street, address.city, address.state, address.country)

      compare(actual, expected)
    }

    "handle 'SELECT  zip as Z, *, state as S FROM simpleAddress' from record" in {
      val address = SimpleAddress("Rock St", "MtV", "CA", "94041", "USA")

      val schema = SchemaFor[SimpleAddress]()
      val toRecord = ToRecord[SimpleAddress]
      val record = RecordFormat[SimpleAddress].to(address)

      val actual = record.sql("SELECT zip as Z, *, state as S FROM simpleAddress")

      case class LocalSimpleAddress(Z: String, street: String, city: String, country: String, S: String)
      val expected = LocalSimpleAddress(address.zip, address.street, address.city, address.country, address.state)

      compare(actual, expected)
    }
  }
}
