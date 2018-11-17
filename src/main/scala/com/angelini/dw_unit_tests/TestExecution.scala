package com.angelini.dw_unit_tests

object TestExecution {

  sealed trait Result {
    val failed: Boolean
  }

  case class Success() extends Result {
    val failed = false
  }

  case class Failure(message: String) extends Result {
    val failed = true
  }

  case class PartitionResults(result: Result,
                              partitionResults: Map[Partition, Result])
    extends Result {

    val failed: Boolean = result.failed

    override def toString: String = result.toString
  }

}
