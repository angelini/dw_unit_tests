package com.angelini.dw_unit_tests

object Runner {
  type Results = Map[Dataset, Map[TestCase, TestExecution.Result]]

  def displayResults(results: Results): Unit = {
    println("-----")
    for ((dataset, testResults) <- results) {
      println(s"${dataset.root}")
      for ((test, partitionResults) <- testResults) {
        println(s"   ${test.getClass.getName}")
        for ((partition, result) <- partitionResults) {
          println(s"        ${partition.path} $result")
        }
      }
    }
    println("-----")
  }
}

class Runner(cases: Map[Dataset, Seq[TestCase]] = Map(),
             cache: Runner.Results = Map()) {

  def copy(cases: Map[Dataset, Seq[TestCase]] = cases,
           cache: Runner.Results = cache) = new Runner(cases, cache)

  def withTestsFor(dataset: Dataset, caseSeq: Seq[TestCase]): Runner =
    copy(cases = cases + (dataset -> caseSeq))

  def withCache(cache: Runner.Results): Runner = copy(cache = cache)

  def execute(): Runner.Results =
    cases.map {
      case (dataset, tests) =>
        println(s"Executing ${tests.length} tests for $dataset")
        val cachedResults = cache.getOrElse(dataset, Map())
        val results = tests.map {
          case test: PartitionTestCase =>
            val cachedResult = cachedResults.getOrElse(test, Map())
            val partitionResults = dataset.partitions.map { partition =>
              (partition, cachedResult.getOrElse(partition, test.run(partition)))
            }
            (test, partitionResults.toMap)
          case test: DatasetTestCase =>
            (test, cachedResults.getOrElse(test, test.run(dataset)))
        }
        (dataset, results.toMap)
    }
}
