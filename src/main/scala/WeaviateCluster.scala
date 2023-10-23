package io.weaviate.spark

import org.apache.spark.sql.connector.catalog.{SupportsWrite, SupportsRead, TableCapability}
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.connector.read.{ScanBuilder}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.types.StructType

import java.util
import scala.jdk.CollectionConverters._

case class WeaviateCluster(weaviateOptions: WeaviateOptions, schema: StructType) extends SupportsWrite with SupportsRead {
  override def name(): String = weaviateOptions.className

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    WeaviateWriteBuilder(weaviateOptions, info.schema())
  }

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    if (weaviateOptions.partitionColumn != "") {
      WeaviateScanBuilder(weaviateOptions, schema)
    } else {
      throw new WeaviateOptionsError("partitionColumn option is required in order to read and it must be a 32bit integer")
    }
  }

  override def capabilities(): util.Set[TableCapability] = Set(
    TableCapability.BATCH_WRITE,
    TableCapability.STREAMING_WRITE,
    TableCapability.BATCH_READ
  ).asJava
}
