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

package com.twitter.summingbird.online.executor

import com.twitter.algebird.{SummingQueue, Semigroup, MapAlgebra}
import com.twitter.bijection.Injection
import com.twitter.util.Future

import com.twitter.summingbird.online.Externalizer

import com.twitter.summingbird.online.{FlatMapOperation, AsyncCache, CacheBuilder}
import com.twitter.summingbird.option.CacheSize
import com.twitter.summingbird.online.option.{
  MaxWaitingFutures,
  MaxFutureWaitTime,
  MaxEmitPerExecute,
  FlushFrequency
}


/**
 * @author Oscar Boykin
 * @author Sam Ritchie
 * @author Ashu Singhal
 * @author Ian O Connell
 */

class FinalFlatMap[Event, Key, Value, S, D](
  @transient flatMapOp: FlatMapOperation[Event, (Key, Value)],
  cacheBuilder: CacheBuilder[Key, (List[InputState[S]], Value)],
  maxWaitingFutures: MaxWaitingFutures,
  maxWaitingTime: MaxFutureWaitTime,
  maxEmitPerExec: MaxEmitPerExecute,
  pDecoder: Injection[Event, D],
  pEncoder: Injection[(Key, Value), D]
  )
  (implicit monoid: Semigroup[Value])
    extends AsyncBase[Event, (Key, Value), InputState[S], D](maxWaitingFutures,
                                                          maxWaitingTime,
                                                          maxEmitPerExec) {
  val encoder = pEncoder
  val decoder = pDecoder

  val lockedOp = Externalizer(flatMapOp)

  lazy val sCache: AsyncCache[Key, (List[InputState[S]], Value)] = cacheBuilder(implicitly[Semigroup[(List[InputState[S]], Value)]])


  private def formatResult(outData: Map[Key, (List[InputState[S]], Value)])
                        : Iterable[(List[InputState[S]], Future[TraversableOnce[(Key, Value)]])] = {
    outData.toList.map{ case (key, (tupList, value)) =>
      (tupList, Future.value(List((key, value))))
    }
  }

  override def tick: Future[Iterable[(List[InputState[S]], Future[TraversableOnce[(Key, Value)]])]] = {
    sCache.tick.map(formatResult(_))
  }

  def cache(state: InputState[S],
            items: TraversableOnce[(Key, Value)]): Future[Iterable[(List[InputState[S]], Future[TraversableOnce[(Key, Value)]])]] = {

    val itemL = items.toList
    if(itemL.size > 0) {
      state.fanOut(itemL.size - 1) // Since input state starts at a 1
      sCache.insert(itemL.map{case (k, v) => k -> (List(state), v)}).map(formatResult(_))
    }
    else { // Here we handle mapping to nothing, option map et. al
        Future.value(
          List(
            (List(state), Future.value(Nil))
          )
        )
      }
  }

  override def apply(state: InputState[S],
                     tup: Event) =
    lockedOp.get.apply(tup).map { cache(state, _) }.flatten

  override def cleanup {
    lockedOp.get.close
    sCache.cleanup
  }
}
