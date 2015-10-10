package docs.stream.cookbook

import akka.stream.OverflowStrategy
import akka.stream.scaladsl._
import akka.stream.testkit._

import scala.collection.immutable
import scala.concurrent.Await
import scala.concurrent.duration._

class RecipeDroppyBroadcast extends RecipeSpec {

  "Recipe for a droppy broadcast" must {
    "work" in {
      val pub = TestPublisher.probe[Int]()
      val myElements = Source(pub)

      val sub1 = TestSubscriber.manualProbe[Int]()
      val sub2 = TestSubscriber.manualProbe[Int]()
      val sub3 = TestSubscriber.probe[Int]()
      val futureSink = Sink.head[Seq[Int]]
      val mySink1 = Sink(sub1)
      val mySink2 = Sink(sub2)
      val mySink3 = Sink(sub3)

      //#droppy-bcast
      val graph = FlowGraph.runnable(mySink1, mySink2, mySink3)((_, _, _)) { implicit b =>
        (sink1, sink2, sink3) =>
          import FlowGraph.Implicits._

          val bcast = b.add(Broadcast[Int](3))
          myElements ~> bcast

          bcast.buffer(10, OverflowStrategy.dropHead) ~> sink1
          bcast.buffer(10, OverflowStrategy.dropHead) ~> sink2
          bcast.buffer(10, OverflowStrategy.dropHead) ~> sink3
      }
      //#droppy-bcast

      graph.run()

      sub3.request(100)
      for (i <- 1 to 100) {
        pub.sendNext(i)
        sub3.expectNext(i)
      }

      pub.sendComplete()

      sub1.expectSubscription().request(10)
      sub2.expectSubscription().request(10)

      for (i <- 91 to 100) {
        sub1.expectNext(i)
        sub2.expectNext(i)
      }

      sub1.expectComplete()
      sub2.expectComplete()

    }
  }

}
