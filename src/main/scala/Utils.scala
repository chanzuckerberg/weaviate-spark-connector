package io.weaviate.spark

import java.util
import io.weaviate.client.v1.schema.model.Property
import org.apache.spark.sql.types.{DataType, DataTypes, Metadata, StructField}

object Utils {
  def weaviateToSparkDatatype(datatype: util.List[String]): DataType = {
    datatype.get(0) match {
      case "string[]" => DataTypes.createArrayType(DataTypes.StringType)
      case "int" => DataTypes.IntegerType
      case "int[]" => DataTypes.createArrayType(DataTypes.IntegerType)
      case "boolean" => DataTypes.BooleanType
      case "boolean[]" => DataTypes.createArrayType(DataTypes.BooleanType)
      case "number" => DataTypes.DoubleType
      case "number[]" => DataTypes.createArrayType(DataTypes.DoubleType)
      case "date" => DataTypes.DateType
      case "date[]" => DataTypes.createArrayType(DataTypes.DateType)
      case "text[]" => DataTypes.createArrayType(DataTypes.StringType)
      case default => DataTypes.StringType
    }
  }

  def weaviateToSparkStructField(property: Property, options: WeaviateOptions): StructField = {
    val propertyType = property.getDataType.get(0)
    val isTimestampColumn = options.timestampColumns.contains(property.getName)

    val dataType = if (isTimestampColumn) {
      propertyType match {
        case "date" => DataTypes.TimestampType
        case "date[]" => DataTypes.createArrayType(DataTypes.TimestampType)
        case default => throw new SparkDataTypeNotSupported(s"Conversion from ${propertyType} to timestamp is not supported")
      }
    }
    else {
      Utils.weaviateToSparkDatatype(property.getDataType)
    }

    StructField(property.getName, dataType, true, Metadata.empty)
  }
}
