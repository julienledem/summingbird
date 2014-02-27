package com.twitter.summingbird.unascribed

import com.twitter.summingbird.batch.option.{ FlatMapShards, Reducers }
import com.twitter.summingbird.scalding.source.TimePathedSource
import com.twitter.summingbird.batch._
import com.twitter.summingbird.chill._
import com.twitter.summingbird._
import com.twitter.summingbird.option._
import com.twitter.algebird.Semigroup
import com.twitter.algebird.monad.{StateWithError, Reader}
import com.twitter.summingbird._
import com.twitter.summingbird.option._

object Unascribed {

  def apply(jobName: String) = {
    new Unascribed(jobName)
  }

  implicit def source[T](dataSource: UnascribedDataSource[T]): Producer[Unascribed, T] =
    Producer.source[Unascribed, T](dataSource)

  def datasource[T](name: String) = new UnascribedDataSource[T](name)

  def service[K, V](name: String) = new UnascribedService[K, V](name)

  def store[K, V](name: String) = new UnascribedStore[K, V](name)

}

class Unascribed(val jobName: String) extends Platform[Unascribed] {

  type Source[T] = UnascribedDataSource[T]
  type Store[K, V] = UnascribedStore[K, V]
  type Sink[T] = UnascribedDataSource[T]
  type Service[K, V] = UnascribedService[K, V]
  type Plan[T] = UnascribedPlan[T]

  def plan[T](completed: TailProducer[Unascribed, T]): Plan[T] = {
    new UnascribedPlan(completed)
  }

}

class UnascribedService[K, +V](name: String)

class UnascribedStore[K, V](name: String)

class UnascribedDataSource[T](name: String)

class UnascribedPlan[T](completed: TailProducer[Unascribed, T]) {

  def toSpecificPlatform[P <: Platform[P]](platform: P) : P#Plan[T] =
    platform.plan(toPlatformPlan(platform, completed))

  private def toPlatformPlan[P <: Platform[P]](platform: P, completed: TailProducer[Unascribed, T]): TailProducer[P, T] = {

    // TODO: actual lookup
    def toPlatformSource[T](source: UnascribedDataSource[T]): Producer[P, T] = null
    def toPlatformSink[T](sink: UnascribedDataSource[T]): P#Sink[T] = null.asInstanceOf[P#Sink[T]]
    def toPlatformService[K, V](service: UnascribedService[K, V]): P#Service[K,V] = null.asInstanceOf[P#Service[K, V]]
    def toPlatformStore[K, V](store: UnascribedStore[K, V]): P#Store[K,V] = null.asInstanceOf[P#Store[K, V]]

    def toPlatformProducer[T, K, V](outerProducer: Producer[Unascribed, T], jamfs: Map[Producer[Unascribed, _], Producer[P, _]]): (Producer[P, T], Map[Producer[Unascribed, _], Producer[P, _]]) = {
      def toPlatformProducerMap[T, U](producer: Producer[Unascribed, T], f: (Producer[P, T]) => (Producer[P, U])): (Producer[P, U], Map[Producer[Unascribed, _], Producer[P, _]]) = {
    	val (s, m) = toPlatformProducer(producer, jamfs)
    	(f(s), m)
      }
      jamfs.get(outerProducer) match {
        case Some(s) => (s.asInstanceOf[Producer[P, T]], jamfs)
        case None =>
          val (s, m) = outerProducer match {
            case NamedProducer(producer, name) => toPlatformProducerMap[T, T](producer, _.name(name))
            case IdentityKeyedProducer(producer) =>
              val (s, m) = toPlatformProducer(producer, jamfs)
              (new IdentityKeyedProducer(s), m)
            case Source(source) => (toPlatformSource(source), jamfs)
            case OptionMappedProducer(producer, fn) => toPlatformProducerMap[Any, T](producer, _.optionMap(fn(_)))
            case FlatMappedProducer(producer, fn) => toPlatformProducerMap[Any, T](producer, _.flatMap(fn(_)))
            case MergedProducer(l, r) =>
              val (leftS, leftM) = toPlatformProducer(l, jamfs)
              val (rightS, rightM) = toPlatformProducer(r, leftM)
              (leftS.merge(rightS), rightM)
            case KeyFlatMappedProducer(producer, fn) =>
              val (s, m) = toPlatformProducer(producer, jamfs)
              (s.flatMapKeys(fn), m)
            case AlsoProducer(l, r) =>
              val (left, leftM) = toPlatformProducer(l, jamfs)
              val (right, rightM) = toPlatformProducer(r, leftM)
              (left.asInstanceOf[TailProducer[P, T]].also(right), rightM)
            case WrittenProducer(producer, sink) => toPlatformProducerMap[T, T](producer, _.write(toPlatformSink(sink)))
            case LeftJoinedProducer(producer, service) =>
              val (s, m) = toPlatformProducer(producer, jamfs)
              (s.leftJoin(toPlatformService(service)), m)
            case Summer(producer, store, monoid) =>
              val (s, m) = toPlatformProducer(producer, jamfs)
              (s.sumByKey(toPlatformStore(store))(monoid), m)
          }
          (s.asInstanceOf[Producer[P, T]], m + (outerProducer -> s))
      }
    }
    toPlatformProducer(completed, Map.empty)._1.asInstanceOf[TailProducer[P, T]] // TODO fix instanceof
  }




}