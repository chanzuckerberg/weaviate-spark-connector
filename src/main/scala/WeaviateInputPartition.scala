package io.weaviate.spark

import io.weaviate.client.v1.filters.Operator
import io.weaviate.client.v1.filters.WhereFilter
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory}

case class WeaviateInputPartition(className: String, partitionColumn: String, start: Int, stop: Int) extends InputPartition {

	def toWhereFilter() = {
	    val startFilter = WhereFilter.builder()
	    	.path(partitionColumn)
	        .operator(Operator.GreaterThanEqual)
	        .valueInt(start)
	        .build()

	    val stopFilter = WhereFilter.builder()
	    	.path(partitionColumn)
	        .operator(Operator.LessThanEqual)
	        .valueInt(stop)
	        .build()

	    val operands = Array(startFilter, stopFilter)

	    WhereFilter.builder()
	        .operator(Operator.And)
	        .operands(operands:_*)
	        .build()
	}
}