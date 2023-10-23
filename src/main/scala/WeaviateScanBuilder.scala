package io.weaviate.spark

import org.apache.spark.internal.Logging
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.expressions.{NamedReference, Literal}
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder, SupportsPushDownV2Filters}

// org.apache.spark.sql.connector.expressions.FieldReference, org.apache.spark.sql.connector.expressions.LiteralValue

case class WeaviateScanBuilder(weaviateOptions: WeaviateOptions, schema: StructType)
  extends ScanBuilder
  with SupportsPushDownV2Filters
  with Serializable
  with Logging {

  private var pushedPredicate = Array.empty[Predicate]

  override def build(): Scan = WeaviateScan(weaviateOptions, schema, pushedPredicate)

  override def pushPredicates(predicates: Array[Predicate]): Array[Predicate] = {
    predicates.zipWithIndex.foreach { case(predicate, index) =>
      val children = predicate.children

      val operands = children.map{ expression =>
        expression match {
          case reference: NamedReference => s"reference = ${reference.fieldNames.toSeq}"
          case literal: Literal[Any] => s"literal = ${literal.dataType}:${literal.value}"
          case _ => s"expression = ${expression.describe}, type = ${expression.getClass.getName}"
        }
      }

      logInfo(f"""${index}%02d: ${predicate.name}%15s => [${operands.mkString(", ")}]""")

      // if (
      //     children.length == 2 &&
      //     children(0).isInstanceOf[NamedReference] &&
      //     children(1).isInstanceOf[Literal[Any]]
      //   ) {
      //   val reference = children(0).asInstanceOf[NamedReference]
      //   val literal = children(1).asInstanceOf[Literal[Any]]

      //   logInfo(s"name = ${predicate.name}, reference = ${reference.fieldNames.toSeq}, literal = ${literal.dataType}:${literal.value}")
      // }
    }
    predicates
  }

  override def pushedPredicates(): Array[Predicate] = pushedPredicate
}
