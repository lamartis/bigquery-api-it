package integration

import com.google.cloud.bigquery._
import org.slf4j.LoggerFactory
import org.apache.spark.sql.types._
import collection.JavaConversions._

/**
  * A tool to convert a DataFrame Schema to BigQuery Table Schema.
  *
  * @author saadlamarti
  */

object SchemaConverter {
  private lazy val logger = LoggerFactory.getLogger(getClass)

  /**
    * Convert a StructType Spark SQL Schema to TableDefinition Big Query Table Schema
    *
    * @param schema
    * @return
    */
  def sqlToBQSchema(schema: StructType): TableDefinition = {
    val fields = schema.map(field => sqlTypeToBQType(field, field.dataType)).toList
    StandardTableDefinition.of(Schema.of(fields))
  }

  /**
    * Convert a Spark Field Representation (org.apache.spark.sql.type.StructField)
    * into a Big Query Field (com.google.cloud.bigquery.Field)
    *
    * @param field
    * @return
    */
  def sqlTypeToBQType(field: StructField, dataType: DataType): Field =
    dataType match {
      case structType: StructType =>
        fieldBuilder(field, dataType, structType.fields.map(field => sqlTypeToBQType(field, field.dataType)).toList: _*)
      case arrayType: ArrayType =>
        arrayType.elementType match {
          case _: ArrayType =>
            throw new IllegalArgumentException(s"Multidimensional arrays are not supported")
          case other =>
            sqlTypeToBQType(field, other)
        }
      case mapType: MapType =>
        throw new IllegalArgumentException(s"Unsupported type: ${field.dataType}")
      case other => fieldBuilder(field, other)
    }

  /**
    * Build a BigQuery Field
    *
    * @param structField
    * @param fields
    * @return
    */
  private def fieldBuilder(structField: StructField, dataType: DataType, fields: Field*): Field = {
    val bqDataType: LegacySQLTypeName = getBQType(dataType)
    val mode = getMode(structField)

    Field.newBuilder(structField.name, bqDataType, fields: _*).setMode(mode).build()
  }

  /**
    * Get the field Mode
    * - REPEATED: is used if the Spark SQL Field dataType is an ArrayType
    * - NULLABLE: is used if the Spark SQL Field is Nullable
    * - REQUIRED: is used if the Spark SQL Field is mustn't be Nullable
    *
    * @param field
    * @return
    */
  private def getMode(field: StructField) = {
    field.dataType match {
      case ArrayType(_, _) => Field.Mode.REPEATED
      case _ => if (field.nullable) Field.Mode.NULLABLE else Field.Mode.REQUIRED
    }
  }

  /**
    * Here is the mapping between the Spark SQL Data Types and Big Query Data Types
    *
    * @see for more details: https://cloud.google.com/bigquery/data-types
    * @param dataType
    * @return
    */
  private def getBQType(dataType: DataType) = dataType match {
    case ByteType | ShortType | IntegerType | LongType => LegacySQLTypeName.INTEGER
    case StringType => LegacySQLTypeName.STRING
    case FloatType | DoubleType => LegacySQLTypeName.FLOAT
    case _: DecimalType => LegacySQLTypeName.FLOAT
    case BinaryType => LegacySQLTypeName.BYTES
    case BooleanType => LegacySQLTypeName.BOOLEAN
    case TimestampType => LegacySQLTypeName.TIMESTAMP
    case DateType => LegacySQLTypeName.DATE
    case _: ArrayType | _: StructType => LegacySQLTypeName.RECORD
    case _ => throw new RuntimeException(s"Couldn't match type $dataType")
  }

}

