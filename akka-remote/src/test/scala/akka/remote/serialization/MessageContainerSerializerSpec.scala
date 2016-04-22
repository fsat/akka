/**
 * Copyright (C) 2009-2016 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.remote.serialization

import akka.serialization.SerializationExtension
import akka.testkit.AkkaSpec
import akka.actor._

class MessageContainerSerializerSpec extends AkkaSpec {

  val ser = SerializationExtension(system)

  "MessageContainerSerializer" must {
    Seq(
      classOf[ActorSelectionMessage].getSimpleName -> ActorSelectionMessage("hello", Vector(
        SelectChildName("user"), SelectChildName("a"), SelectChildName("b"), SelectParent,
        SelectChildPattern("*"), SelectChildName("c")), wildcardFanOut = true),
      classOf[Identify].getSimpleName -> Identify("some-message"),
      s"${classOf[ActorIdentity].getSimpleName} without actor ref" -> ActorIdentity("some-message", ref = None)).foreach {
        case (scenario, item) ⇒
          s"resolve serializer for $scenario" in {
            ser.serializerFor(item.getClass).getClass should ===(classOf[MessageContainerSerializer])
          }

          s"serialize and de-serialize $scenario" in {
            verifySerialization(item)
          }
      }

    def verifySerialization(msg: AnyRef): Unit = {
      ser.deserialize(ser.serialize(msg).get, msg.getClass).get should ===(msg)
    }

  }
}

