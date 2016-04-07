package BIDMach

import BIDMach.datasources.IteratorSource
import BIDMach.datasources.IteratorSource.{Options => IteratorOpts}
import BIDMach.models.Model
import BIDMach.updaters.Batch
import BIDMat.{MatIO, SerText}
import BIDMat.SciFunctions._
import org.apache.spark.rdd.RDD
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkContext
import scala.reflect.ClassTag

object RunOnSpark{
  // Called copy the reduced learner into a RDD of learners.
  def mapToMockLearner(learner:MockLearner)(l: Iterator[Int]): Iterator[MockLearner] = {
    Iterator[MockLearner](learner)
  }

  // Instantiates a learner based on the parameters of the learner that is passed in and binds the data to the new learner.
  def firstPass(l: MockLearner)(rdd_data:Iterator[(SerText, BIDMat.MatIO)]):Iterator[MockLearner] = {
    val i_opts = new IteratorSource.Options
    i_opts.iter = rdd_data
    val iteratorSource = new IteratorSource(i_opts)
    val learner = new MockLearner(iteratorSource, l.model, l.mixins, l.updater, l.datasink)
    learner.firstPass(null)
    Iterator[MockLearner](learner)
  }

  // Runs subsequent passes of the learner.
  def nextPass(data_iterator: Iterator[(SerText, BIDMat.MatIO)], learner_iterator: Iterator[MockLearner]):Iterator[MockLearner] = {
    val learner = learner_iterator.next
    learner.nextPass(data_iterator)
    Iterator[MockLearner](learner)
  }

  def wrapUpMockLearner(learner_iterator: Iterator[MockLearner]): Iterator[MockLearner] = {
    val learner = learner_iterator.next
    learner.wrapUp
    Iterator[MockLearner](learner)
  }

  // Gathers and reduces all the learners into one learner.
  def reduce_fn(ipass: Int)(l: MockLearner, r: MockLearner): MockLearner = {
    l.model.combineModels(ipass, r.model)
    l
  }

  def runOnSpark(sc: SparkContext, learner:MockLearner, rdd_data:RDD[(SerText,MatIO)], num_partitions: Int):RDD[MockLearner] = {
    // Instantiate a learner, run the first pass, and reduce all of the learners' models into one learner.
    var reduced_learner = rdd_data.mapPartitions[MockLearner](firstPass(learner), preservesPartitioning=true)
                                  .treeReduce(reduce_fn(0), 2)
    // Once we've reduced our distributed learners into one learner, we can update our model.
    reduced_learner.updateM

    // Redistribute our reduced learner across all partitions
    var rdd_learner: RDD[MockLearner] = sc.parallelize(1 to num_partitions)
                                      .coalesce(num_partitions)
                                      .mapPartitions[MockLearner](mapToMockLearner(reduced_learner), preservesPartitioning=true)

    rdd_learner.persist()
    
    // While we still have more passes to complete
    for (i <- 1 until learner.opts.npasses) {
      // Call nextPass on each learner and reduce the learners into one learner
      val t0 = System.nanoTime()
      reduced_learner = rdd_data.zipPartitions(rdd_learner, preservesPartitioning=true)(nextPass)
                                .treeReduce(reduce_fn(i), 2)
      val t1 = System.nanoTime()
      println("Elapsed time iter " + i + ": " + (t1 - t0)/math.pow(10, 9)+ "s")
      // Update the model
      reduced_learner.updateM
      // Redistribute the learner across all partitions
      rdd_learner = sc.parallelize(1 to num_partitions)
                      .coalesce(num_partitions)
                      .mapPartitions[MockLearner](mapToMockLearner(reduced_learner), preservesPartitioning=true)
      rdd_learner.persist()
    }
    // Note: The returned RDD has a transformation applied to it.
    rdd_learner = rdd_learner.mapPartitions(wrapUpMockLearner, preservesPartitioning=true)
    rdd_learner
  }
}
