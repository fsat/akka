/**
 * Copyright (C) 2009-2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.remote.serialization

import akka.remote.transport.AkkaPduCodec.Payload

import scala.collection.immutable
import akka.protobuf.ByteString
import akka.actor._
import akka.remote.ContainerFormats
import akka.serialization.SerializationExtension
import akka.serialization.BaseSerializer
import akka.serialization.SerializerWithStringManifest

class MessageContainerSerializer(val system: ExtendedActorSystem) extends BaseSerializer {

  @deprecated("Use constructor with ExtendedActorSystem", "2.4")
  def this() = this(null)

  private lazy val serialization = SerializationExtension(system)

  // TODO remove this when deprecated this() is removed
  override val identifier: Int =
    if (system eq null) 6
    else identifierFromConfig

  def includeManifest: Boolean = false

  def toBinary(obj: AnyRef): Array[Byte] = obj match {
    case identify: Identify         ⇒ serializeIdentify(identify)
    case identity: ActorIdentity    ⇒ serializeActorIdentity(identity)
    case sel: ActorSelectionMessage ⇒ serializeSelection(sel)
    case _                          ⇒ throw new IllegalArgumentException(s"Cannot serialize object of type [${obj.getClass.getName}]")
  }

  import ContainerFormats.PatternType._

  private def serializeSelection(sel: ActorSelectionMessage): Array[Byte] = {
    val builder = ContainerFormats.SelectionEnvelope.newBuilder()
    val message = sel.msg.asInstanceOf[AnyRef]
    val serializer = serialization.findSerializerFor(message)
    builder.
      setEnclosedMessage(ByteString.copyFrom(serializer.toBinary(message))).
      setSerializerId(serializer.identifier).
      setWildcardFanOut(sel.wildcardFanOut)

    serializer match {
      case ser2: SerializerWithStringManifest ⇒
        val manifest = ser2.manifest(message)
        if (manifest != "")
          builder.setMessageManifest(ByteString.copyFromUtf8(manifest))
      case _ ⇒
        if (serializer.includeManifest)
          builder.setMessageManifest(ByteString.copyFromUtf8(message.getClass.getName))
    }

    sel.elements.foreach {
      case SelectChildName(name) ⇒
        builder.addPattern(buildPattern(Some(name), CHILD_NAME))
      case SelectChildPattern(patternStr) ⇒
        builder.addPattern(buildPattern(Some(patternStr), CHILD_PATTERN))
      case SelectParent ⇒
        builder.addPattern(buildPattern(None, PARENT))
    }

    builder.build().toByteArray
  }

  private def serializeIdentify(identify: Identify): Array[Byte] =
    ContainerFormats.Identify.newBuilder()
      .setMessageId(payloadBuilder(identify.messageId))
      .build()
      .toByteArray

  private def serializeActorIdentity(actorIdentity: ActorIdentity): Array[Byte] =
    ContainerFormats.ActorIdentity.newBuilder()
      .setCorrelationId(payloadBuilder(actorIdentity.correlationId))
      // TODO: serialize ref field
      .build()
      .toByteArray

  private def payloadBuilder(input: Any): ContainerFormats.Payload.Builder = {
    val payload = input.asInstanceOf[AnyRef]
    val builder = ContainerFormats.Payload.newBuilder()
    val serializer = serialization.findSerializerFor(payload)

    builder
      .setEnclosedMessage(ByteString.copyFrom(serializer.toBinary(payload)))
      .setSerializerId(serializer.identifier)

    serializer match {
      case ser2: SerializerWithStringManifest ⇒
        val manifest = ser2.manifest(payload)
        if (manifest != "")
          builder.setMessageManifest(ByteString.copyFromUtf8(manifest))
      case _ ⇒
        if (serializer.includeManifest)
          builder.setMessageManifest(ByteString.copyFromUtf8(payload.getClass.getName))
    }

    builder
  }

  private def buildPattern(matcher: Option[String], tpe: ContainerFormats.PatternType): ContainerFormats.Selection.Builder = {
    val builder = ContainerFormats.Selection.newBuilder().setType(tpe)
    matcher foreach builder.setMatcher
    builder
  }

  def fromBinary(bytes: Array[Byte], manifest: Option[Class[_]]): AnyRef = {
    manifest match {
      case Some(forClass) if forClass == classOf[ActorSelectionMessage] ⇒ deserializeSelection(bytes)
      case Some(forClass) if forClass == classOf[Identify] ⇒ deserializeIdentify(bytes)
      case Some(forClass) if forClass == classOf[ActorIdentity] ⇒ deserializeActorIdentity(bytes)
      case Some(unsupported) ⇒ throw new IllegalArgumentException(s"Cannot deserialize object of type [${unsupported.getName}]")
      case None ⇒ throw new IllegalArgumentException(s"Cannot deserialize object of unknown type")
    }
  }

  private def deserializeSelection(bytes: Array[Byte]): ActorSelectionMessage = {
    val selectionEnvelope = ContainerFormats.SelectionEnvelope.parseFrom(bytes)
    val manifest = if (selectionEnvelope.hasMessageManifest) selectionEnvelope.getMessageManifest.toStringUtf8 else ""
    val msg = serialization.deserialize(
      selectionEnvelope.getEnclosedMessage.toByteArray,
      selectionEnvelope.getSerializerId,
      manifest).get

    import scala.collection.JavaConverters._
    val elements: immutable.Iterable[SelectionPathElement] = selectionEnvelope.getPatternList.asScala.map { x ⇒
      x.getType match {
        case CHILD_NAME    ⇒ SelectChildName(x.getMatcher)
        case CHILD_PATTERN ⇒ SelectChildPattern(x.getMatcher)
        case PARENT        ⇒ SelectParent
      }

    }(collection.breakOut)
    val wildcardFanOut = if (selectionEnvelope.hasWildcardFanOut) selectionEnvelope.getWildcardFanOut else false
    ActorSelectionMessage(msg, elements, wildcardFanOut)
  }

  private def deserializeIdentify(bytes: Array[Byte]): Identify = {
    val identify = ContainerFormats.Identify.parseFrom(bytes)
    val messageId = deserializePayload(identify.getMessageId)
    Identify(messageId)
  }

  private def deserializeActorIdentity(bytes: Array[Byte]): ActorIdentity = {
    val actorIdentity = ContainerFormats.ActorIdentity.parseFrom(bytes)
    val correlationId = deserializePayload(actorIdentity.getCorrelationId)
    // TODO: deserialize ref field
    ActorIdentity(correlationId, None)
  }

  private def deserializePayload(payload: ContainerFormats.Payload): Any = {
    val manifest = if (payload.hasMessageManifest) payload.getMessageManifest.toStringUtf8 else ""
    serialization.deserialize(
      payload.getEnclosedMessage.toByteArray,
      payload.getSerializerId,
      manifest).get
  }

}
