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
      ActorSelectionMessage("hello", Vector(
        SelectChildName("user"), SelectChildName("a"), SelectChildName("b"), SelectParent,
        SelectChildPattern("*"), SelectChildName("c")), wildcardFanOut = true),
      Identify("some-message")).foreach { item ⇒
        s"resolve serializer for ${item.getClass.getSimpleName}" in {
          ser.serializerFor(item.getClass).getClass should ===(classOf[MessageContainerSerializer])
        }

        s"serialize and de-serialize ${item.getClass.getSimpleName}" in {
          verifySerialization(item)
        }
      }

    def verifySerialization(msg: AnyRef): Unit = {
      ser.deserialize(ser.serialize(msg).get, msg.getClass).get should ===(msg)
    }

  }
}

