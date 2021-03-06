package integration

import com.google.cloud.bigquery._
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

/**
  * A Helper tools to manipulate BigQuery requests
  *
  * @author saad LAMARTI
  */

object BigQueryHelper {

  private lazy val logger = LoggerFactory.getLogger(getClass)

  /**
    *
    * @param dataset
    * @param bigQuery
    * @return
    */
  def doesDatasetAlreadyExist(dataset: String)(implicit bigQuery: BigQuery): Boolean =
    bigQuery.getDataset(dataset) != null

  /**
    *
    * @param schema
    * @param bigQuery
    */
  def createTable(schema: StructType, tableId: TableId, bigQuery: BigQuery): Unit = {
    var remoteTable = bigQuery.getTable(tableId)

    if (remoteTable == null) {
      val tableDefinition = SchemaConverter.sqlToBQSchema(schema)
      val info = TableInfo.newBuilder(tableId, tableDefinition).build()

      remoteTable = bigQuery.create(info)
      logger.debug(s"Table '${tableId.getDataset}:${tableId.getTable}' has been well created")
    } else
      logger.debug(s"Table '${tableId.getDataset}:${tableId.getTable}' Already created")
  }


  /**
    * Check if the table is already exist
    *
    * @param tableId
    * @param bigQuery
    * @return
    */
  def doesTableAlreadyExist(tableId: TableId)(implicit bigQuery: BigQuery): Boolean = {
    val value = bigQuery.getTable(tableId) != null
    if (value)
      logger.debug(s"Table '${tableId.getDataset}:${tableId.getTable}' exists")
    else
      logger.debug(s"Table '${tableId.getDataset}:${tableId.getTable}' doesn't exist")
    value
  }

}
