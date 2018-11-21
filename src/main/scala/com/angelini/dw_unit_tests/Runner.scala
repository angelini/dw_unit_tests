package com.angelini.dw_unit_tests

import com.angelini.dw_unit_tests.sampler.{AlwaysSampler, Sampler}

object Runner {
  type Results = Map[Dataset, Map[TestCase, TestExecution.Result]]

  def displayResults(results: Results): Unit = {
    println("-----")
    for ((dataset, testResults) <- results) {
      println(s"${dataset.root}")
      for ((test, result) <- testResults) {
        println(s"   ${test.getClass.getName} [$result]")
        result match {
          case r: TestExecution.PartitionResults => {
            for ((partition, pResult) <- r.partitionResults) {
              val relative = partition.path.subpath(
                dataset.root.getNameCount,
                partition.path.getNameCount
              )
              println(s"        $relative $pResult")
            }
          }
          case _ => Unit
        }
      }
      println("")
    }
  }
}

class Runner(cases: Map[Dataset, Seq[TestCase]] = Map(),
             sampler: Sampler = AlwaysSampler) {

  def copy(cases: Map[Dataset, Seq[TestCase]] = cases,
           sampler: Sampler = sampler) = new Runner(cases, sampler)

  def withTestsFor(datasets: Seq[Dataset], caseSeq: Seq[TestCase]): Runner =
    copy(cases =
      (cases.keySet ++ datasets.toSet).map(dataset => {
        (dataset, cases.get(dataset).map(_ ++ caseSeq).getOrElse(caseSeq))
      }).toMap
    )

  def withSampler(sampler: Sampler): Runner = copy(sampler = sampler)

  def execute(): Runner.Results =
    cases.filter {
      case (dataset, _) => sampler.includeDataset(dataset)
    }.map { case (dataset, tests) =>
      println(s"Executing ${tests.length} tests for ${dataset.root}")
      val results = tests.par.map { test =>
        (test, executeTestCase(test, dataset))
      }
      (dataset, results.seq.toMap)
    }

  private def executeTestCase(test: TestCase, dataset: Dataset): TestExecution.Result =
    test match {
      case test: PartitionTestCase =>
        val partitionResults = dataset.partitions
          .filter(sampler.includePartition(dataset, _))
          .map { partition =>
            (partition, test.run(partition))
          }.toMap
        TestExecution.PartitionResults(
          if (partitionResults.values.exists(_.failed))
            TestExecution.Failure("Partition test failure")
          else
            TestExecution.Success(),
          partitionResults
        )

      case test: DatasetTestCase => test.run(dataset)
    }
}
