package BIDMach

import BIDMat.{MatIO, SerText}
import BIDMat.SciFunctions._
import org.apache.spark.rdd.RDD
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkContext

import scala.reflect.ClassTag

object RunOnSpark{
  def mapToLearner(learner:Learner)(l: Iterator[Int]): Iterator[Learner] = {
    Iterator[Learner](learner);
  }

  def firstPass(learner: Learner)(rdd_data:Iterator[(SerText, BIDMat.MatIO)]):Iterator[Learner] = {
    learner.firstPass(rdd_data)
    learner.updateM
    Iterator[Learner](learner)
  }

  def nextPass(data_iterator: Iterator[(SerText, BIDMat.MatIO)], learner_iterator: Iterator[Learner]):Iterator[Learner] = {
    val learner = learner_iterator.next
    learner.nextPass(data_iterator)
    learner.updateM
    Iterator[Learner](learner)
  }

  def wrapUpLearner(learner_iterator: Iterator[Learner]): Iterator[Learner] = {
    val learner = learner_iterator.next
    learner.wrapUp
    Iterator[Learner](learner)
  }

  def reduce_fn(ipass: Int)(l: Learner, r: Learner): Learner = {
    l.model.combineModels(ipass, r.model)
    l
  }

  def runOnSpark(sc: SparkContext, learner:Learner, rdd_data:RDD[(SerText,MatIO)], num_partitions: Int):RDD[Learner] = {
    var reduced_learner = rdd_data.mapPartitions[Learner](firstPass(learner), preservesPartitioning=true)
                                  .reduce(reduce_fn(0))
    var rdd_learner: RDD[Learner] = sc.parallelize(1 to num_partitions)
                                      .coalesce(num_partitions)
                                      .mapPartitions[Learner](mapToLearner(reduced_learner), preservesPartitioning=true)
    for (i <- 1 until learner.opts.npasses) {
      reduced_learner = rdd_data.zipPartitions(rdd_learner, preservesPartitioning=true)(nextPass)
                                .reduce(reduce_fn(i))
      reduced_learner.updateM
      rdd_learner = sc.parallelize(1 to num_partitions)
                      .coalesce(num_partitions)
                      .mapPartitions[Learner](mapToLearner(reduced_learner), preservesPartitioning=true)
    }
    rdd_learner = rdd_learner.mapPartitions(wrapUpLearner, preservesPartitioning=true)
    rdd_learner
  }
}
