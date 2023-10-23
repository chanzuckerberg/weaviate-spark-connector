package io.weaviate.spark

import java.time.Instant
import java.time.ZonedDateTime
import java.util.TimeZone
import java.sql.Timestamp
import scala.collection.mutable
import scala.collection.Iterator
import scala.collection.JavaConverters._
import org.apache.spark.internal.Logging
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.catalyst.util.SparkDateTimeUtils
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.execution.datasources.v2.PartitionReaderFromIterator
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReader, PartitionReaderFactory}
import io.weaviate.client.v1.graphql.model.GraphQLError
import io.weaviate.client.v1.graphql.query.fields.Field

case class WeaviateReaderFactory(weaviateOptions: WeaviateOptions, schema: StructType)
  extends PartitionReaderFactory with Logging {

  def buildInternalRow(properties: Map[String, AnyRef]): GenericInternalRow = {
    val timeZone = TimeZone.getTimeZone(weaviateOptions.timeZone).toZoneId
    val additional = properties("_additional").asInstanceOf[java.util.Map[String, AnyRef]]

    val values = schema.fields.map{field =>
      if (field.name == weaviateOptions.id) {
        UTF8String.fromString(additional.get("id").toString)
      }
      else if (field.name == weaviateOptions.vector) {
        val vector = additional.get("vector").asInstanceOf[java.util.ArrayList[Double]]
        ArrayData.toArrayData(vector.asScala.map(_.toFloat))
      }
      else {
        val value = properties(field.name)
        field.dataType match {
          case DataTypes.StringType => {
            if (value != null) {
              UTF8String.fromString(value.toString)
            } else {
              null
            }
          }
          case DataTypes.BooleanType => value.asInstanceOf[Boolean]
          case DataTypes.IntegerType => value.asInstanceOf[Double].toInt
          case DataTypes.DoubleType => value.asInstanceOf[Double]
          case DataTypes.LongType => value.asInstanceOf[Long]
          case DataTypes.DateType => {
            if (value != null) {
              val zonedDateTime = ZonedDateTime.parse(value.toString)
              val date = java.sql.Date.valueOf(zonedDateTime.toLocalDate)
              SparkDateTimeUtils.fromJavaDate(date)
            } else {
              null
            }
          }
          case DataTypes.TimestampType => {
            if (value != null) {
              val zonedDateTime = ZonedDateTime.parse(value.toString)
              zonedDateTime.toInstant.toEpochMilli * 1000
            } else {
              null
            }
          }
          case _ => throw SparkDataTypeNotSupported(s"${field.dataType} is not supported for reading")
        }

      }
    }
    new GenericInternalRow(values)
  }

  def buildQueryFields(): Seq[Field] = {
    val partitionColumn = weaviateOptions.partitionColumn

    val fieldNames = mutable.Set[String]()
    fieldNames ++= schema.fieldNames
    fieldNames += partitionColumn

    val additionalFieldNames = mutable.Buffer[String]()
    if (weaviateOptions.id != null) {
      additionalFieldNames += "id"
      fieldNames -= weaviateOptions.id
    }
    if (weaviateOptions.vector != null) {
      additionalFieldNames += "vector"
      fieldNames -= weaviateOptions.vector
    }

    if (additionalFieldNames.size > 0)
      fieldNames += s"""_additional { ${additionalFieldNames.mkString(" ")} }"""

    fieldNames.map(prop => Field.builder().name(prop).build).toSeq
  }

  def readPartition(partition: WeaviateInputPartition): Seq[GenericInternalRow] = {
    val queryMaximumResults = weaviateOptions.queryMaximumResults
    val consistencyLevel = weaviateOptions.consistencyLevel
    val className = weaviateOptions.className

    val client = weaviateOptions.getClient

    val query = client.graphQL().get
      .withClassName(className)
      .withFields(buildQueryFields:_*)
      .withWhere(partition.toWhereFilter)
      .withLimit(queryMaximumResults)

    val results = if (consistencyLevel != "") {
      query.withConsistencyLevel(consistencyLevel).run()
    } else {
      query.run()
    }

    if (results.hasErrors) {
      throw new WeaviateResultError(s"${results.getError.getMessages}")
    }

    val response = results.getResult
    val errors = Option(response.getErrors).getOrElse(Array[GraphQLError]())

    if (errors.size > 0) {
      throw new WeaviateResultError(s"""${errors.map(error => error.getMessage).mkString(", ")}""")
    }

    val items = (
      response.getData.asInstanceOf[java.util.Map[String, Object]].
      get("Get").asInstanceOf[java.util.Map[String, AnyRef]].
      get(className).asInstanceOf[java.util.ArrayList[java.util.Map[String, AnyRef]]].asScala.toList.
      map(_.asScala.toMap)
    )

    items.map(item => buildInternalRow(item))
  }

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    val rows = readPartition(partition.asInstanceOf[WeaviateInputPartition])
    new PartitionReaderFromIterator(rows.iterator)
  }
}