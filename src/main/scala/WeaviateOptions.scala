package io.weaviate.spark

import WeaviateOptions._

import java.util.TimeZone
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import io.weaviate.client.{Config, WeaviateClient, WeaviateAuthClient}
import scala.collection.JavaConverters._
import io.weaviate.client.v1.data.replication.model.ConsistencyLevel

class WeaviateOptions(config: CaseInsensitiveStringMap) extends Serializable {
  private val DEFAULT_BATCH_SIZE = 100
  private val DEFAULT_RETRIES = 2
  private val DEFAULT_RETRIES_BACKOFF = 2
  private val DEFAULT_TIMEOUT_SECONDS = 60

  val batchSize: Int =
    config
      .getInt(WEAVIATE_BATCH_SIZE_CONF, DEFAULT_BATCH_SIZE)
  val host: String = config.get(WEAVIATE_HOST_CONF)
  val scheme: String = config.get(WEAVIATE_SCHEME_CONF)
  val className: String = config.get(WEAVIATE_CLASSNAME_CONF)
  val tenant: String = config.getOrDefault(WEAVIATE_TENANT_CONF, null)
  val vector: String = config.get(WEAVIATE_VECTOR_COLUMN_CONF)
  val id: String = config.get(WEAVIATE_ID_COLUMN_CONF)
  val retries: Int = config.getInt(WEAVIATE_RETRIES_CONF, DEFAULT_RETRIES)
  val retriesBackoff: Int = config.getInt(WEAVIATE_RETRIES_BACKOFF_CONF, DEFAULT_RETRIES_BACKOFF)
  val timeout: Int = config.getInt(WEAVIATE_TIMEOUT, DEFAULT_TIMEOUT_SECONDS)
  val oidcUsername: String = config.getOrDefault(WEAVIATE_OIDC_USERNAME, "")
  val oidcPassword: String = config.getOrDefault(WEAVIATE_OIDC_PASSWORD, "")
  val oidcClientSecret: String = config.getOrDefault(WEAVIATE_OIDC_CLIENT_SECRET, "")
  val oidcAccessToken: String = config.getOrDefault(WEAVIATE_OIDC_ACCESS_TOKEN, "")
  val oidcAccessTokenLifetime: Long = config.getLong(WEAVIATE_OIDC_ACCESS_TOKEN_LIFETIME, 0)
  val oidcRefreshToken: String = config.getOrDefault(WEAVIATE_OIDC_REFRESH_TOKEN, "")
  val apiKey: String = config.getOrDefault(WEAVIATE_API_KEY, "")
  val consistencyLevel: String = {
    val value = config.getOrDefault(WEAVIATE_CONSISTENCY_LEVEL, "")
    if (value != "" && value != ConsistencyLevel.ONE && value != ConsistencyLevel.QUORUM && value != ConsistencyLevel.ALL)
      throw new WeaviateOptionsError(s"Invalid consistency level: ${value}")
    value
  }
  val partitionColumn: String = config.getOrDefault(WEAVIATE_PARTITION_COLUMN, "")
  val queryMaximumResults: Int = config.getInt(WEAVIATE_QUERY_MAXIMUM_RESULTS, 100000)
  val timeZone: String = {
    val value = config.getOrDefault(WEAVIATE_TIME_ZONE, TimeZone.getDefault.getID)
    if (!TimeZone.getAvailableIDs.contains(value))
      throw new WeaviateOptionsError(s"Invalid time zone: ${value}")
    value
  }
  val numPartitions: Int = config.getInt(WEAVIATE_NUM_PARTITIONS, -1)
  val timestampColumns: Set[String] = {
    Option(config.get(WEAVIATE_TIMESTAMP_COLUMNS)).map(value =>
      value.split(",").map(_.trim).toSet
    ).getOrElse(Set[String]())
  }

  var headers: Map[String, String] = Map()
  config.forEach((option, value) => {
    if (option.startsWith(WEAVIATE_HEADER_PREFIX)) {
      headers += (option.replace(WEAVIATE_HEADER_PREFIX, "") -> value)
    }
  })

  // var client: WeaviateClient = _

  def getClient(): WeaviateClient = {
    // if (client != null) return client
    val config = new Config(scheme, host, headers.asJava, timeout, timeout, timeout)

    if (!oidcUsername.trim().isEmpty() && !oidcPassword.trim().isEmpty()) {
      WeaviateAuthClient.clientPassword(config, oidcUsername, oidcPassword, null)
    } else if (!oidcClientSecret.trim().isEmpty()) {
      WeaviateAuthClient.clientCredentials(config, oidcClientSecret, null)
    } else if (!oidcAccessToken.trim().isEmpty()) {
      WeaviateAuthClient.bearerToken(config, oidcAccessToken, oidcAccessTokenLifetime, oidcRefreshToken)
    } else if (!apiKey.trim().isEmpty()) {
      WeaviateAuthClient.apiKey(config, apiKey)
    } else {
      new WeaviateClient(config)
    }
  }
}

object WeaviateOptions {
  val WEAVIATE_BATCH_SIZE_CONF: String = "batchSize"
  val WEAVIATE_HOST_CONF: String       = "host"
  val WEAVIATE_SCHEME_CONF: String     = "scheme"
  val WEAVIATE_CLASSNAME_CONF: String  = "className"
  val WEAVIATE_TENANT_CONF: String  = "tenant"
  val WEAVIATE_VECTOR_COLUMN_CONF: String  = "vector"
  val WEAVIATE_ID_COLUMN_CONF: String  = "id"
  val WEAVIATE_RETRIES_CONF: String = "retries"
  val WEAVIATE_RETRIES_BACKOFF_CONF: String = "retriesBackoff"
  val WEAVIATE_TIMEOUT: String = "timeout"
  val WEAVIATE_OIDC_USERNAME: String = "oidc:username"
  val WEAVIATE_OIDC_PASSWORD: String = "oidc:password"
  val WEAVIATE_OIDC_CLIENT_SECRET: String = "oidc:clientSecret"
  val WEAVIATE_OIDC_ACCESS_TOKEN: String = "oidc:accessToken"
  val WEAVIATE_OIDC_ACCESS_TOKEN_LIFETIME: String = "oidc:accessTokenLifetime"
  val WEAVIATE_OIDC_REFRESH_TOKEN: String = "oidc:refreshToken"
  val WEAVIATE_API_KEY: String = "apiKey"
  val WEAVIATE_HEADER_PREFIX: String = "header:"
  val WEAVIATE_CONSISTENCY_LEVEL: String = "consistencyLevel"
  val WEAVIATE_PARTITION_COLUMN: String = "partitionColumn"
  val WEAVIATE_QUERY_MAXIMUM_RESULTS: String = "queryMaximumResults"
  val WEAVIATE_TIME_ZONE: String = "timeZone"
  val WEAVIATE_NUM_PARTITIONS: String = "numPartitions"
  val WEAVIATE_TIMESTAMP_COLUMNS: String = "timestampColumns"
}
