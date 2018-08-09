package integration

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

  def PROJECT_ID = BigqueryServiceProvider.PROJECT_ID
  def DATASET_CI_TESTS = "dataset_ci_tests"
  def TABLE_FOR_TESTS = "bq_sink_table"

  def tableId = TableId.of(PROJECT_ID, DATASET_CI_TESTS, TABLE_FOR_TESTS)

  val schema = StructType(Seq(StructField("id", StringType, true),
    StructField("name", StringType, true),
    StructField("reference", StringType, true)))

  implicit val bigquery: BigQuery = BigqueryServiceProvider.build()

  "BQ target target table" should "not be available" in {
    // Delete table whether it is already created
    bigquery.delete(tableId)
    BigQueryHelper.doesTableAlreadyExist(tableId) should equal(false)
  }

  it should "be well created, and feed by 6 rows" in {
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

    // Most of the time the insertAll function seems well executed, without throwing any exception,
    // however, data is never sent to BQ. How it can possible ?
    val insertResponse = bigquery.insertAll(insertRequest)

    // Check if errors occurred
    if (insertResponse.hasErrors)
      logger.debug("Errors occurred while inserting rows")
    else
      logger.debug("Data has been inserted without errors")
  }

  it should "have 6 rows" in {
    // I have added the Thread.sleep here, as some times, when the data insertion is done correctly,
    // the table rows count is equal to 0. This means that the the insertion request is not yet finished, and the
    // BQ API doesn't treat well the asynchronous requests ?
    Thread.sleep(4000)
    bigquery.listTableData(tableId).iterateAll().asScala.size should equal(6)
  }

}
