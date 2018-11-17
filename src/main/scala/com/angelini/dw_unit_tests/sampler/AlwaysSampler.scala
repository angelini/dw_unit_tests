package com.angelini.dw_unit_tests.sampler
import com.angelini.dw_unit_tests.{Dataset, Partition}

object AlwaysSampler extends Sampler {
  override def includeDataset(dataset: Dataset): Boolean = true

  override def includePartition(dataset: Dataset, partition: Partition): Boolean = true
}
