package com.angelini.dw_unit_tests

import com.angelini.dw_unit_tests.store.Store

sealed trait TestCase

trait PartitionTestCase extends TestCase {
  def run(store: Store, dataset: Dataset, partition: Partition): TestExecution.Result
}

trait DatasetTestCase extends TestCase {
  def run(store: Store, dataset: Dataset): TestExecution.Result
}
