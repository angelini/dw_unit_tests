package com.angelini.dw_unit_tests.sampler
import com.angelini.dw_unit_tests.{Dataset, Partition}

class RandomSampler(datasetRate: Double, partitionRate: Double) extends Sampler {
  override def includeDataset(dataset: Dataset): Boolean =
    scala.util.Random.nextDouble() <= datasetRate

  override def includePartition(dataset: Dataset, partition: Partition): Boolean =
    scala.util.Random.nextDouble() <= partitionRate
}
