package integration

import com.google.cloud.bigquery.BigQueryOptions
import com.google.cloud.bigquery.testing.RemoteBigQueryHelper

/**
  *
  *
  * @author saadlamarti
  */

object BQAuthMode extends Enumeration {
  val DefaultAuth, ServiceAccountAuth = Value
}

object BigqueryServiceProvider {
  def PROJECT_ID = "xxx-id"

  var defaultAuth = BQAuthMode.ServiceAccountAuth

  def build() = defaultAuth match {
    case BQAuthMode.ServiceAccountAuth => {
      RemoteBigQueryHelper.create(PROJECT_ID, getClass.getResourceAsStream("/service-account-key.json"))
        .getOptions.getService
    }
    case BQAuthMode.DefaultAuth => BigQueryOptions.getDefaultInstance.getService
  }
}
