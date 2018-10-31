package com.angelini.dw_unit_tests

object TestExecution {

  sealed trait PartitionResult

  case class Success() extends PartitionResult

  case class Failure(message: String) extends PartitionResult

  type Result = Map[Partition, PartitionResult]

}
