package com.twitter.summingbird.unascribed

import com.twitter.summingbird._
import org.specs2.mutable._
import org.specs2.runner.JUnitRunner
import org.junit.runner.RunWith

@RunWith(classOf[JUnitRunner])
class TestUnascribedPlatform extends Specification {
  import com.twitter.summingbird.unascribed.Unascribed._

  val fooDatasource = datasource[String]("foo")
  val barDatasource = datasource[Int]("bar")
  val bazService = service[Int, (Int, String)]("baz")
  val blahStore = store[Int, Int]("blah")

  val platform = new Unascribed("my job")

  "TESTING ... " in {
    val plan = platform.plan(
     fooDatasource
      .map(_.length)
      .lookup(bazService).map((t) => (t._1, t._1))
      .sumByKey(blahStore).map(_._1)
      .write(barDatasource)
      )
    plan.toSpecificPlatform(platform) must not(throwA[Exception])
  }
}
