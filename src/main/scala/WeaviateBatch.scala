package io.weaviate.spark

import org.apache.spark.internal.Logging
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.catalyst.expressions.{EqualTo}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory}
import org.apache.spark.sql.connector.expressions.filter.Predicate
import io.weaviate.client.v1.graphql.model.GraphQLError
import io.weaviate.client.v1.graphql.query.fields.Field

case class WeaviateBatch(weaviateOptions: WeaviateOptions, schema: StructType, predicates: Array[Predicate])
  extends Batch
  with Serializable
  with Logging {

  def readPartitionColumnBounds(): (Int, Int) = {
    val partitionColumn = weaviateOptions.partitionColumn
    val className = weaviateOptions.className

    val client = weaviateOptions.getClient

    val field = Field.builder()
      .name(weaviateOptions.partitionColumn)
      .fields(
        Field.builder.name("minimum").build(),
        Field.builder.name("maximum").build()
      ).build()

    val results = client.graphQL().aggregate
      .withClassName(className)
      .withFields(field)
      .run

    if (results.hasErrors) {
      throw new WeaviateResultError(s"${results.getError.getMessages}")
    }

    val response = results.getResult

    val errors = Option(response.getErrors).getOrElse(Array[GraphQLError]())

    if (errors.size > 0) {
      throw new WeaviateResultError(s"""${errors.map(error => error.getMessage).mkString(", ")}""")
    }

    val data = response.getData.asInstanceOf[java.util.Map[String, Object]]
    val aggregate = data.get("Aggregate").asInstanceOf[java.util.Map[String, Object]]
    val klass = aggregate.get(className).asInstanceOf[java.util.List[java.util.Map[String, Object]]]
    val stats = klass.get(0).asInstanceOf[java.util.Map[String, Object]].get(partitionColumn)

    val start = stats.asInstanceOf[java.util.Map[String,Double]].get("minimum").toInt
    val stop = stats.asInstanceOf[java.util.Map[String,Double]].get("maximum").toInt

    (start, stop)
  }


  override def planInputPartitions(): Array[InputPartition] = {
    val partitionColumn = weaviateOptions.partitionColumn
    val className = weaviateOptions.className

    val queryMaximumResults = weaviateOptions.queryMaximumResults
    val (minimum, maximum) = readPartitionColumnBounds()
    val estimatedCount = maximum - minimum + 1
    val numberOfCores = java.lang.Runtime.getRuntime().availableProcessors

    val defaultNumPartitions = if (estimatedCount > queryMaximumResults) {
      estimatedCount / queryMaximumResults
    } else if (estimatedCount > numberOfCores) {
      numberOfCores
    } else {
      4
    }

    val numPartitions = Seq(defaultNumPartitions, weaviateOptions.numPartitions).max
    val rowsPerPartition = estimatedCount / numPartitions

    // logInfo(s"numPartitions = ${numPartitions}")
    // logInfo(s"estimatedCount = ${estimatedCount}")
    // logInfo(s"rowsPerPartition = ${rowsPerPartition}")

    (0 to numPartitions - 1).map{ index =>
      val start = minimum + index * rowsPerPartition
      val stop = if (index == numPartitions - 1) maximum else start + rowsPerPartition - 1
      WeaviateInputPartition(className, partitionColumn, start, stop)
    }.toArray
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    WeaviateReaderFactory(weaviateOptions, schema)
  }
}
