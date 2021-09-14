package org.example.command

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{Row, SparkSession}

case class ShowVersionCommand() extends RunnableCommand {

  override val output: Seq[Attribute] =
    Seq(AttributeReference("version", StringType, nullable = true)())

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val outputString = System.getenv("SPARK_USER")
    Seq(Row(outputString))
  }
}
