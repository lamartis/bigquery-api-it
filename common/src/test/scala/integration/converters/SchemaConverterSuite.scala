package integration.converters

import com.google.cloud.bigquery._
import com.google.cloud.bigquery.Field
import integration.SchemaConverter
import org.apache.spark.sql.types._
import org.scalatest.{FlatSpec, Matchers}

import collection.JavaConversions._

/**
  * SchemaConverter Tool test implementations
  *
  * @author saadlamarti
  */
class SchemaConverterSuite extends FlatSpec with Matchers {

  "The converted Spark SQL Map field" should "throw an exception, not supported dataType on BQ" in {
    val mapField = StructField("myMap", MapType(StringType, StringType), true)
    assertThrows[IllegalArgumentException] {
      SchemaConverter.sqlTypeToBQType(mapField, mapField.dataType)
    }
  }

  "The converted Spark SQL Array[simple dataType] field" should "have the same expected BQ field representation" in {
    val arrayField = StructField("myArray", ArrayType(StringType), true)
    val convertedField = SchemaConverter.sqlTypeToBQType(arrayField, arrayField.dataType)

    val expectedBQField = Field.newBuilder("myArray", LegacySQLTypeName.STRING)
      .setMode(Field.Mode.REPEATED)
      .build()

    convertedField should equal(expectedBQField)
  }

  "The converted Spark SQL Array[complex dataType] field" should "have the same expected BQ field representation" in {
    val arrayField = StructField("myArray", ArrayType(StructType(Seq(
      StructField("field2-1", IntegerType, false),
      StructField("field2-2", BinaryType, true),
      StructField("field2-3", BooleanType, false)
    ))), true)
    val convertedField = SchemaConverter.sqlTypeToBQType(arrayField, arrayField.dataType)

    val expectedBQField = Field.newBuilder("myArray", LegacySQLTypeName.RECORD,
      Seq(Field.newBuilder("field2-1", LegacySQLTypeName.INTEGER).setMode(Field.Mode.REQUIRED).build(),
        Field.newBuilder("field2-2", LegacySQLTypeName.BYTES).setMode(Field.Mode.NULLABLE).build(),
        Field.newBuilder("field2-3", LegacySQLTypeName.BOOLEAN).setMode(Field.Mode.REQUIRED).build()): _*)
      .setMode(Field.Mode.REPEATED)
      .build()

    convertedField should equal(expectedBQField)
  }

  "The converted SparkSQL Schema" should "have the same expected BQ schema representation" in {
    val st = StructType(Seq(
      StructField("field1", StringType, true),
      StructField("field2", StructType(Seq(
        StructField("field2-1", IntegerType, false),
        StructField("field2-2", BinaryType, true),
        StructField("field2-3", LongType, true),
        StructField("field2-4", ShortType, true),
        StructField("field2-5", BooleanType, false))))))

    val expectedBQSchema = StandardTableDefinition.of(Schema.of(Seq(
      Field.newBuilder("field1", LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build(),
      Field.newBuilder("field2", LegacySQLTypeName.RECORD,
        Seq(Field.newBuilder("field2-1", LegacySQLTypeName.INTEGER).setMode(Field.Mode.REQUIRED).build(),
          Field.newBuilder("field2-2", LegacySQLTypeName.BYTES).setMode(Field.Mode.NULLABLE).build(),
          Field.newBuilder("field2-3", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build(),
          Field.newBuilder("field2-4", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build(),
          Field.newBuilder("field2-5", LegacySQLTypeName.BOOLEAN).setMode(Field.Mode.REQUIRED).build()): _*
      ).setMode(Field.Mode.NULLABLE).build())))

    SchemaConverter.sqlToBQSchema(st) should equal(expectedBQSchema)
  }

}