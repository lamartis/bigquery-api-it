package integration

import java.util.UUID

import com.google.cloud.bigquery._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.scalatest.prop.Checkers
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

/**
  * Integration Tests for Big Query API
  *
  * @author saadlamarti
  */

class BigQueryIntegration extends FlatSpec with Checkers with BeforeAndAfterAll with Matchers {
  private lazy val logger = LoggerFactory.getLogger(getClass)

  val PROJECT_ID = BigqueryServiceProvider.PROJECT_ID
  val DATASET_CI_TESTS = "dataset_ci_tests"
  val TABLE_FOR_TESTS = "bq_sink_table_tmp_" + UUID.randomUUID().toString.replace('-', '_')

  val tableId = TableId.of(PROJECT_ID, DATASET_CI_TESTS, TABLE_FOR_TESTS)

  val schema = StructType(Seq(StructField("id", StringType, true),
    StructField("name", StringType, true),
    StructField("reference", StringType, true)))

  implicit val bigquery: BigQuery = BigqueryServiceProvider.build()

  "BQ target target table" should "be well created, and feed by 6 rows" in {
    BigQueryHelper.createTable(schema, tableId, bigquery)
    BigQueryHelper.doesTableAlreadyExist(tableId) should equal(true)

    val insertRequest = InsertAllRequest.newBuilder(tableId,
      Seq(InsertAllRequest.RowToInsert.of("1", Map("id" -> "1", "name" -> "name1", "reference" -> "reference1").asJava),
        InsertAllRequest.RowToInsert.of("2", Map("id" -> "2", "name" -> "name2", "reference" -> "reference2").asJava),
        InsertAllRequest.RowToInsert.of("3", Map("id" -> "3", "name" -> "name3", "reference" -> "reference3").asJava),
        InsertAllRequest.RowToInsert.of("4", Map("id" -> "4", "name" -> "name4", "reference" -> "reference4").asJava),
        InsertAllRequest.RowToInsert.of("5", Map("id" -> "5", "name" -> "name5", "reference" -> "reference5").asJava),
        InsertAllRequest.RowToInsert.of("6", Map("id" -> "6", "name" -> "name6", "reference" -> "reference6").asJava)
      ): _*)
      .setSkipInvalidRows(true)
      .setIgnoreUnknownValues(true)
      .build()

    val insertResponse = bigquery.insertAll(insertRequest)

    // Check if errors occurred
    if (insertResponse.hasErrors)
      logger.debug("Errors occurred while inserting rows")
    else
      logger.debug("Data has been inserted without errors")
  }

  it should "have 6 rows" in {
    // As The BigQuery Streaming system's table metadata is in an eventually consistent mode,
    // there is a small latency to find the whole sent data on Big Query.
    Thread.sleep(4000)
    bigquery.listTableData(tableId).iterateAll().asScala.size should equal(6)
  }

  it should "not be available" in {
    bigquery.delete(tableId)
    BigQueryHelper.doesTableAlreadyExist(tableId) should equal(false)
  }

}
