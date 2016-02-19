package BIDMach

import BIDMach.Learner.Options
import BIDMach.datasources.IteratorSource
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
  def mapToLearner(learner:Learner)(l: Iterator[Int]): Iterator[Learner] = {
    Iterator[Learner](learner)
  }

  // Instantiates a learner based on the model passed in and runs the first pass.
  def firstPass(model: Model)(rdd_data:Iterator[(SerText, BIDMat.MatIO)]):Iterator[Learner] = {
    val i_opts = new IteratorSource.Options
    i_opts.iter = rdd_data
    val iteratorSource = new IteratorSource(i_opts)
    val learner = new Learner(iteratorSource, model, null, new Batch(), null)
    learner.firstPass(rdd_data)
    Iterator[Learner](learner)
  }

  // Runs subsequent passes of the learner.
  def nextPass(data_iterator: Iterator[(SerText, BIDMat.MatIO)], learner_iterator: Iterator[Learner]):Iterator[Learner] = {
    val learner = learner_iterator.next
    learner.nextPass(data_iterator)
    Iterator[Learner](learner)
  }

  def wrapUpLearner(learner_iterator: Iterator[Learner]): Iterator[Learner] = {
    val learner = learner_iterator.next
    learner.wrapUp
    Iterator[Learner](learner)
  }

  // Gathers and reduces all the learners into one learner.
  def reduce_fn(ipass: Int)(l: Learner, r: Learner): Learner = {
    l.model.combineModels(ipass, r.model)
    l
  }

  def runOnSpark(sc: SparkContext, model:Model, rdd_data:RDD[(SerText,MatIO)], num_partitions: Int):RDD[Learner] = {
    // Instantiate a learner, run the first pass, and reduce all of the learners' models into one learner.
    var reduced_learner = rdd_data.mapPartitions[Learner](firstPass(model), preservesPartitioning=true)
                                  .reduce(reduce_fn(0))
    // Once we've reduced our distributed learners into one learner, we can update our model.
    reduced_learner.updateM

    // Redistribute our reduced learner across all partitions
    var rdd_learner: RDD[Learner] = sc.parallelize(1 to num_partitions)
                                      .coalesce(num_partitions)
                                      .mapPartitions[Learner](mapToLearner(reduced_learner), preservesPartitioning=true)

    // While we still have more passes to complete
    for (i <- 1 until new Options().npasses) {
      // Call nextPass on each learner and reduce the learners into one learner
      reduced_learner = rdd_data.zipPartitions(rdd_learner, preservesPartitioning=true)(nextPass)
                                .reduce(reduce_fn(i))
      // Update the model
      reduced_learner.updateM
      // Redistribute the learner across all partitions
      rdd_learner = sc.parallelize(1 to num_partitions)
                      .coalesce(num_partitions)
                      .mapPartitions[Learner](mapToLearner(reduced_learner), preservesPartitioning=true)
    }
    // Note: The returned RDD has a transformation applied to it.
    rdd_learner = rdd_learner.mapPartitions(wrapUpLearner, preservesPartitioning=true)
    rdd_learner
  }
}
