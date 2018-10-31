package com.angelini.dw_unit_tests

trait TestCase

trait PartitionTestCase extends TestCase {
  def run(partition: Partition): TestExecution.PartitionResult
}

trait DatasetTestCase extends TestCase {
  def run(dataset: Dataset): TestExecution.Result
}
