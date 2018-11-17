package com.angelini.dw_unit_tests

import com.angelini.dw_unit_tests.sampler.{AlwaysSampler, Sampler}

object Runner {
  type Results = Map[Dataset, Map[TestCase, TestExecution.Result]]

  def displayResults(results: Results): Unit = {
    println("-----")
    for ((dataset, testResults) <- results) {
      println(s"${dataset.root}")
      for ((test, partitionResults) <- testResults) {
        println(s"   ${test.getClass.getName}")
        for ((partition, result) <- partitionResults) {
          val relative = partition.path.subpath(
            dataset.root.getNameCount,
            partition.path.getNameCount
          )
          println(s"        $relative $result")
        }
      }
    }
    println("")
  }
}

class Runner(cases: Map[Dataset, Seq[TestCase]] = Map(),
             sampler: Sampler = AlwaysSampler,
             cache: Runner.Results = Map()) {

  def copy(cases: Map[Dataset, Seq[TestCase]] = cases,
           sampler: Sampler = sampler,
           cache: Runner.Results = cache) = new Runner(cases, sampler, cache)

  def withTestsFor(dataset: Dataset, caseSeq: Seq[TestCase]): Runner =
    copy(cases = cases + (dataset -> caseSeq))

  def withSampler(sampler: Sampler): Runner = copy(sampler = sampler)

  def withCache(cache: Runner.Results): Runner = copy(cache = cache)

  def execute(): Runner.Results =
    cases.filter {
      case (dataset, _) => sampler.includeDataset(dataset)
    }.map {
      case (dataset, tests) =>
        println(s"Executing ${tests.length} tests for ${dataset.root}")
        val cachedResults = cache.getOrElse(dataset, Map())
        val results = tests.map {
          case test: PartitionTestCase =>
            val cachedResult = cachedResults.getOrElse(test, Map())
            val partitionResults = dataset.partitions
              .filter(sampler.includePartition(dataset, _))
              .map { partition =>
                (partition, cachedResult.getOrElse(partition, test.run(partition)))
              }
            (test, partitionResults.toMap)
          case test: DatasetTestCase =>
            (test, cachedResults.getOrElse(test, test.run(dataset)))
        }
        (dataset, results.toMap)
    }
}
