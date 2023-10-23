package io.weaviate.spark

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.connector.read.{Batch, Scan}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.connector.expressions.filter.Predicate

case class WeaviateScan(weaviateOptions: WeaviateOptions, schema: StructType, predicates: Array[Predicate])
  extends Scan
  with Serializable {

  override def readSchema(): StructType = schema
  override def toBatch: Batch = WeaviateBatch(weaviateOptions, schema, predicates)
}
