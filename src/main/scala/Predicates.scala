package io.weaviate.spark

trait WeaviatePredicate {
	def operation(): String
}