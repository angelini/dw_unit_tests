package com.angelini.dw_unit_tests

sealed trait TestCase

trait PartitionTestCase extends TestCase {
  def run(partition: Partition): TestExecution.Result
}

trait DatasetTestCase extends TestCase {
  def run(dataset: Dataset): TestExecution.Result
}
