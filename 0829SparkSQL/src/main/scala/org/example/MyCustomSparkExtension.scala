package org.example

import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.SparkSession
import org.example.customparser.StrictParser

class MyCustomSparkExtension extends (SparkSessionExtensions => Unit) {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectParser({ (session,parser) =>
      new StrictParser(parser)
    })
    extensions.injectOptimizerRule { session =>
      new MyPushDown(session)
    }
  }
}


