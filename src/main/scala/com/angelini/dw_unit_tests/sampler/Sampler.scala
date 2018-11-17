package com.angelini.dw_unit_tests.sampler

import com.angelini.dw_unit_tests.{Dataset, Partition}

trait Sampler {
  def includeDataset(dataset: Dataset): Boolean

  def includePartition(dataset: Dataset, partition: Partition): Boolean
}
