/*
Copyright 2013 Twitter, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package com.twitter.summingbird.storm

import backtype.storm.task.TopologyContext
import backtype.storm.metric.api.CountMetric
import org.slf4j.LoggerFactory
import com.twitter.summingbird.storm.option.{SummerStormMetrics, EnableSummerStoreMetrics}
import com.twitter.storehaus.algebra.{MergeableStore, MergableStatStore, StatReporter}
import com.twitter.util.{Promise, Future}

/**
 *
 * @author Ian O Connell
 */
class StoreMetrics
object StoreMetrics {
  private val logger = LoggerFactory.getLogger(classOf[StoreMetrics])
  val metricNames = List("put", "get", "mergeWithSome", "mergeWithNone", "mergeRequest", "multiMergeRequest", "multiMergeWithSome", "multiMergeWithNone")
  def deliverMetrics(context: TopologyContext, enableSummerStoreMetrics: EnableSummerStoreMetrics, nodeName: String, metricSrc: Promise[Map[String, CountMetric]]) {
    if(enableSummerStoreMetrics.get) {
      logger.error("Registering store metrics")
        metricSrc.setValue(metricNames.foldLeft(Map[String, CountMetric]()) {case (oldMap, curName) =>
          val metricName = "store/%s".format(curName)
          val metric = context.registerMetric(metricName, new CountMetric, 10)
          oldMap + (curName -> metric)
        })
    } else logger.error("Stor metrics disabled")
  }

  def getStatStore[K, V](nodeName: String, metricSrc: Promise[Map[String, CountMetric]], store: MergeableStore[K, V]): MergeableStore[K, V] = {
    logger.warn("Producing stat wrapping store")
    MergableStatStore(store, StormStatReporter(nodeName, metricSrc))
  }
}
object StormStatReporter {
  private val logger = LoggerFactory.getLogger(classOf[StormStatReporter[_, _]])
}

case class StormStatReporter[K, V](nodeName: String, metricSrc: Promise[Map[String, CountMetric]]) extends StatReporter[K, V] {
    override def putSome {
      metricSrc.map(m => m("put").incr())
    }
    override def putNone {
      metricSrc.map(m => m("put").incr())
    }
    override def multiPutSome {
      metricSrc.map(m => m("put").incr())
    }
    override def multiPutNone {
      metricSrc.map(m => m("put").incr())
    }
    override def getPresent {
      metricSrc.map(m => m("get").incr())
    }
    override def getAbsent {
      metricSrc.map(m => m("get").incr())
    }
    override def multiGetPresent {
      metricSrc.map(m => m("get").incr())
    }
    override def multiGetAbsent {
      metricSrc.map(m => m("get").incr())
    }
    override def mergeWithSome {
      metricSrc.map(m => m("mergeWithSome").incr())
    }
    override def mergeWithNone {
      metricSrc.map(m => m("mergeWithNone").incr())
    }
    override def multiMergeWithSome {
      metricSrc.map(m => m("multiMergeWithSome").incr())
    }
    override def multiMergeWithNone {
      metricSrc.map(m => m("multiMergeWithNone").incr())
    }
    override def traceMultiMerge[K1 <: K](request: Map[K1, Future[Option[V]]]): Map[K1, Future[Option[V]]] = {
      metricSrc.map(m => m("multiMergeRequest").incr())
      request
    }
    override def traceMerge(request: Future[Option[V]]): Future[Option[V]] = {
      metricSrc.map(m => m("mergeRequest").incr())
      request
    }
}