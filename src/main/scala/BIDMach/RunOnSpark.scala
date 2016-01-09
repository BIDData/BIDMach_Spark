package BIDMach

import BIDMat.{MatIO, SerText}
import BIDMat.SciFunctions._
import BIDMach._
import org.apache.spark.rdd.RDD
import org.apache.hadoop.io.Text;

import scala.reflect.ClassTag

object RunOnSpark{
  def firstPass(learner: Learner)(rdd_data:Iterator[(SerText, BIDMat.MatIO)]):Iterator[Learner] = {
    learner.firstPass(rdd_data)
    Iterator(learner)
  }

  def nextPass(data_iterator: Iterator[(SerText, BIDMat.MatIO)], learner_iterator: Iterator[Learner]):Iterator[Learner] = {
    val learner = learner_iterator.next
    learner.nextPass(data_iterator)
    Iterator(learner)
  }

  def wrapUpLearner(learner_iterator: Iterator[Learner]): Iterator[Learner] = {
    val learner = learner_iterator.next
    learner.wrapUp
    Iterator(learner)
  }

  def runOnSpark(learner:Learner, rdd_data:RDD[(SerText,MatIO)]):RDD[Learner] = {
    var rdd_learner: RDD[Learner] = rdd_data.mapPartitions[Learner](firstPass(learner), preservesPartitioning=true)
    for (i <- 1 to learner.opts.npasses) {
      rdd_learner = rdd_data.zipPartitions(rdd_learner, preservesPartitioning=true)(nextPass)
    }
    rdd_learner = rdd_learner.mapPartitions(wrapUpLearner, preservesPartitioning=true)
    rdd_learner
  }
}
