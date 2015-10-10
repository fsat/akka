package akka.stream.testkit

import akka.stream.{ ActorMaterializer, ActorMaterializerSettings, Inlet, Outlet }
import akka.stream.scaladsl._
import org.reactivestreams.Publisher
import scala.collection.immutable
import scala.util.control.NoStackTrace
import akka.stream.testkit.Utils._

abstract class TwoStreamsSetup extends BaseTwoStreamsSetup {

  abstract class Fixture(b: FlowGraph.Builder[_]) {
    def left: Inlet[Int]
    def right: Inlet[Int]
    def out: Outlet[Outputs]
  }

  def fixture(b: FlowGraph.Builder[_]): Fixture

  override def setup(p1: Publisher[Int], p2: Publisher[Int]) = {
    val subscriber = TestSubscriber.probe[Outputs]()
    FlowGraph.runnable() { implicit b ⇒
      import FlowGraph.Implicits._
      val f = fixture(b)

      Source(p1) ~> f.left
      Source(p2) ~> f.right
      f.out ~> Sink(subscriber)

    }.run()

    subscriber
  }

}
