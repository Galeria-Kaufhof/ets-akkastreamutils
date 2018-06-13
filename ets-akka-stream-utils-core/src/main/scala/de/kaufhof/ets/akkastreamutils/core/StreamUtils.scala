package de.kaufhof.ets.akkastreamutils.core

import akka.NotUsed
import akka.http.scaladsl.coding.Gzip
import akka.stream.IOResult
import akka.stream.scaladsl.{BroadcastHub, Flow, Keep, Sink, Source}
import akka.util.ByteString
import java.nio.{ByteBuffer, CharBuffer}
import java.nio.charset.{Charset, CodingErrorAction}

import scala.collection.immutable
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}

object StreamUtils {

  /**
    * Akka sources returning IOResult do not fail with exceptions when read/write failes.
    * Wrapping these sources in failingIoSource ensures the stream will fail with an exception in case of i/o errors.
    * Downside is that the source can be used only once!
    *
    * ATTENTION: resulting source can only be used once!
    */
  def failingIoSource[T](src: Source[T, Future[IOResult]])(implicit ec: ExecutionContext): Source[T, NotUsed] = {

    val p: Promise[IOResult] = Promise()

    val failOnIOErrorSource =
      Source
        .fromFuture(p.future)
        .mapConcat(_.status match {
          case Success(_) => List.empty[T]
          case Failure(exc) => throw exc
        })

    src
      .concat(failOnIOErrorSource)
      .mapMaterializedValue{
        ioResultFut => ioResultFut.onComplete(p.complete)
          NotUsed
      }
  }

  /**
    * Create a source that returns it's materialized value as a future. Allows accessing materialzed value
    * before source is materialized.
    *
    * ATTENTION: resulting source/future can only be used once!
    */
  def preMatSource[T, Mat](source: Source[T, Mat]): (Source[T, NotUsed], Future[Mat]) = {
    val p = Promise[Mat]
    val src = source.mapMaterializedValue(mat => {p.success(mat); NotUsed})
    (src, p.future)
  }

  /**
    * Create a source that returns it's materialized future-value. Allows accessing materialzed value
    * before source is materialized.
    *
    * ATTENTION: resulting source/future can only be used once!
    */
  def preMatFutureSource[T, Mat](source: Source[T, Future[Mat]])
                                (implicit ec: ExecutionContext): (Source[T, NotUsed], Future[Mat]) = {
    val p = Promise[Future[Mat]]
    val src = source.mapMaterializedValue(mat => {p.success(mat); NotUsed})
    (src, p.future.flatMap(identity))
  }

  /**
    * Allows decoding a bytestring to string with a given charset.
    */
  def decodeCharFlow(charset: Charset,
                     bufferSize: Int = 8192,
                     onMalformedInput: CodingErrorAction = CodingErrorAction.REPLACE): Flow[ByteString, String, NotUsed] =

    Flow[ByteString].statefulMapConcat{() =>
      val decoder = new CharDecoder(charset, bufferSize, onMalformedInput)
      (bs: ByteString) => {
        val res = decoder.decode(bs.toArray)
        res :: Nil
      }
    }


  /**
    * Ensure for incoming ByteString that ourgoing ByteStrings are at least minLength bytes long.
    * Last package may be smaller.
    */
  def minLengthFlow(minLength: Int = 64*1024): Flow[ByteString, ByteString, NotUsed] =
    scanFinallyFlow[ByteString, ByteString, ByteString](() => ByteString.empty){(bufferBs, newBs) =>
        val nextBs = bufferBs ++ newBs
        if (nextBs.size >= minLength) {
          (ByteString.empty, Some(nextBs))
        } else {
          (nextBs, None)
        }
      }{bufferBs =>
        Some(bufferBs)
      }

  /**
    * Encode a ByteString with gzip, with a given minimal block size (to ensure encoding does not blow up size
    * for small packages)
    */
  def gzipEncodeFlow(minBlockSize: Int = 64*1024): Flow[ByteString, ByteString, NotUsed] =
    Flow[ByteString]
      .via(minLengthFlow(minBlockSize))
      .via(Gzip.encoderFlow)

  private val bom = ByteString(-17, -69, -65)

  /**
    * strip utf-8 bom from ByteString flow
    */
  val stripBomFlow: Flow[ByteString, ByteString, NotUsed] =
      scanFinallyFlow[ByteString, ByteString, (ByteString, Boolean)](() => (ByteString.empty, false)){(state, newBs) =>
        val (bufferBs, bomChecked) = state
        if (bomChecked) {
          (state, Some(newBs))
        } else {
          val nextBs = bufferBs ++ newBs
          if (nextBs.size >= bom.size) {
            if (nextBs.startsWith(bom)) {
              ((ByteString.empty, true), Some(nextBs.drop(bom.size)))
            } else {
              ((ByteString.empty, true), Some(nextBs))
            }
          } else {
            ((nextBs, false), None)
          }
        }
      }{state =>
        Some(state._1)
      }

  //used in fixedBroadcastHub
  private sealed trait ElemOrEnd[+T]
  private case class Elem[T](v: T) extends ElemOrEnd[T]
  private case class BlockPromise(p: Promise[Unit]) extends ElemOrEnd[Nothing]
  private case class Start(p: Promise[Unit]) extends ElemOrEnd[Nothing]

  /**
    * Workaround for Broadcasthub Bug: If the Broadcasthub upstream is closed before the source is used, all elements
    * cached by Broadcasthub so far will be thrown away und the source will contain no elements when using Broadcasthub
    * directly. This method ensures that the flow does not end until the source has been used at least once.
    */
  def fixedBroadcastHub[T](implicit ec: ExecutionContext): Sink[T, Source[T, NotUsed]] = {
    Flow[T]
      .map(Elem(_))
      .prepend(Source.single(()).mapConcat{_ =>
        val promise = Promise[Unit]
        List(Start(promise), BlockPromise(promise))
      })
      .mapAsync(1){
        case BlockPromise(p) => p.future.map(_ => BlockPromise(p))
        case other => Future.successful(other)
      }
      .toMat(BroadcastHub.sink)(Keep.right)
      .mapMaterializedValue{src =>
        src
          .map{
            case Start(p) => p.trySuccess(()); Start(p)
            case other => other
          }
          .collect{
            case Elem(v) => v
          }
      }
  }

  //used in unfoldFinallyFlow / unfoldFinallyAsyncFlow
  private sealed trait UnfoldFinally[+T]
  private case class  Element[T](v: T) extends UnfoldFinally[T]
  private case object End extends UnfoldFinally[Nothing]
  private case class UnfoldFinallyState[T, State](outElem: Option[T], state: Option[State])

  /**
    * Scan an steroids:
    * 1. State is recreated for each materialization
    * 2. Allows customizing emitted elements
    * 3. Allows final emittation of a last element after upstream completes
    *
    * Downstream elements of type Out are emitted if unfold returns (_, Some[Out]).
    * Before the stream is closed finalize is called once allowing to pass down one last element if needed.
    */
  def scanFinallyFlow[In, Out, State](initialState: () => State)
                                     (scan: (State, In) => (State, Option[Out]))
                                     (finalize: State => Option[Out]): Flow[In, Out, NotUsed] =
    Flow[In]
      .map(Element(_))
      .concat(Source.single(End))
      .scan(UnfoldFinallyState[Out, State](None, None)){(state, newElem) =>
        newElem match {
          case Element(e) =>
            val (newState, nextElemOpt) = scan(state.state.getOrElse(initialState()), e)
            UnfoldFinallyState(nextElemOpt, Some(newState))
          case End =>
            state.copy(outElem = finalize(state.state.getOrElse(initialState())))
        }
      }
      .collect{
        case UnfoldFinallyState(Some(elem), _) => elem
      }

  /**
    * Scan an steroids (async version):
    * 1. State is recreated for each materialization
    * 2. Allows customizing emitted elements
    * 3. Allows final emittation of a last element after upstream completes
    *
    * Downstream elements of type Out are emitted if unfold returns (_, Some[Out]).
    * Before the stream is closed finalize is called once allowing to pass down one last element if needed.
    */
  def scanFinallyAsyncFlow[In, Out, State](initialState: () => State)
                                          (unfold: (State, In) => Future[(State, Option[Out])])
                                          (finalize: State => Future[Option[Out]])
                                          (implicit ec: ExecutionContext): Flow[In, Out, NotUsed] =
    Flow[In]
      .map(Element(_))
      .concat(Source.single(End))
      .scanAsync(UnfoldFinallyState[Out, State](None, None)){(state, newElem) =>
        newElem match {
          case Element(e) =>
            unfold(state.state.getOrElse(initialState()), e).map { case (newState, nextElemOpt) =>
              UnfoldFinallyState(nextElemOpt, Some(newState))
            }
          case End =>
            finalize(state.state.getOrElse(initialState())).map(lastElemOpt =>
              state.copy(outElem = lastElemOpt)
            )
        }
      }
      .collect{
        case UnfoldFinallyState(Some(elem), _) => elem
      }

  /**
    * like fold, but zero value is recreated for every materialization
    */
  def statefulFoldFlow[In, State](z: () => State)(f: (State, In) => State): Flow[In, State, NotUsed] =
    scanFinallyFlow[In, State, State](z)((state, elem) => (f(state, elem), None))(Some(_))

  /**
    * Executes the given function every n-th element or after interval with duration d has passed.
    * Ensures that the last element passing the stream successfully will be passed to this function,
    * even if an exception stops the stream.
    *
    * Behaviour:
    *
    * - Stage pauses until Future returned by f is completed => f is guaranteed to be executed in order.
    * - If the function returns a failed future it will be passed to the stream.
    * - If the upstream throws an exception it will be passed downwards too regardless of f throwing.
    */
  def executeWithinFlow[In](n: Int, d: FiniteDuration)
                           (f: In => Future[Unit])
                           (implicit ec: ExecutionContext): Flow[In, In, NotUsed] =

    Flow[In]
      .map(e => Right(e))
      .recover{case exc => Left(exc)}
      // async boundary is needed here so that timer inside groupedWithin is executed in own actor.
      // Otherwise syncrounous execution of code from prev. stages might prevent timer execution in time
      // may be relatet to this issue:
      // https://stackoverflow.com/questions/42845166/why-does-akka-streams-source-groupedwithin-not-respect-the-duration
      .async
      .groupedWithin(n,d)
      .mapAsync(1){elemsOrError =>
        val lastElem = elemsOrError.lastOption
        lastElem match {
          case Some(Right(elem)) =>
            f(elem).map(_ => immutable.Seq(elemsOrError))
          case Some(Left(exc)) =>
            val elemsWithoutErrors = elemsOrError.dropRight(1)
            elemsWithoutErrors
              .headOption
              .flatMap(_.right.toOption.map(f))
              .getOrElse(Future.successful(()))
              //ignore exception thrown by f here because stream execption happend and will throw further down
              //this prevents stream exception from beeing hidden by exception thrown by f
              .recover{case _: Throwable => ()}
              .map(_ => immutable.Seq(elemsWithoutErrors, immutable.Seq(Left(exc))))
          case None =>
            Future.successful(immutable.Seq(elemsOrError))
        }
      }
      //we need two times map concat in case exception happend to pass down successfully processed elems
      //before executing throw
      .mapConcat(identity)
      .mapConcat(elemsOrError => {
        elemsOrError.headOption match {
          case Some(Left(exc)) =>
            throw exc
          case _ =>
            elemsOrError.collect{case Right(elem) => elem}
        }
      })

  /**
    * Folds elements according to user function `f` and zero value `z` and return the folded value on complete AND
    * on stream failure.
    * Result is a tuple with stream success/failure on left, and fold result on the right side.
    */
  def finallyFoldSink[In, Out](z: Out)
                              (f: (Out, In) => Out)
                              (implicit ec: ExecutionContext): Sink[In, (Future[Unit], Future[Out])] =

    Flow[In]
      .map(elem => Right[Throwable, In](elem))
      .recover { case e: Throwable => Left[Throwable, In](e) }
      .fold((z: Out, Option.empty[Throwable])){(aggAndExcOpt, elemOrExc) =>
        elemOrExc match {
          case Left(exc) =>
            (aggAndExcOpt._1, Some(exc)): (Out, Option[Throwable])
          case Right(elem) =>
            (f(aggAndExcOpt._1, elem), aggAndExcOpt._2): (Out, Option[Throwable])
        }
      }
      .toMat(Sink.last)(Keep.right)
      .mapMaterializedValue{res =>
        (
          res.flatMap(_._2 match {
            case Some(exc) => Future.failed(exc)
            case None => Future.successful(())
          }),
          res.map(_._1)
        )
      }

  /**
    * Return the success or failure of stream on the left side of materialzed tuple and the last element that was passed
    * through on the right side of the materialized value tuple.
    */
  def finallyLastOptionSink[In](implicit ec: ExecutionContext): Sink[In, (Future[Unit], Future[Option[In]])] =
    finallyFoldSink(Option.empty[In])((agg, elem) => Some(elem))

  /**
    * Applies a side effect function to each element. Exception of `sideEffect` will be propagated to the stream.
    * Does not modify elements in the stream.
    */
  def sideEffectFlow[In](sideEffect: In => Unit): Flow[In, In, NotUsed] =
    Flow[In].map(elem => {sideEffect(elem); elem})

}

private[core] class CharDecoder(charset: Charset,
                                       bufferSize: Int = 8192,
                                       onMalformedInput: CodingErrorAction = CodingErrorAction.REPLACE) {

  private val decoder = charset.newDecoder().onMalformedInput(onMalformedInput)

  private val inputBuffer = ByteBuffer.allocate(bufferSize)
  private val outputBuffer = CharBuffer.allocate(bufferSize)

  def decode(data: Array[Byte]): String =
    decode(data, 0, data.length)

  private def decode(data: Array[Byte], offset: Int, limit: Int): String = {
    val bufferSpace = inputBuffer.remaining()
    val remaining = limit-offset
    if (remaining <= bufferSpace) {
      decodeChunk(data, offset, remaining)
    } else {
      decodeChunk(data, offset, bufferSpace) + decode(data, offset + bufferSpace, limit)
    }
  }

  private def decodeChunk(data: Array[Byte], offset: Int, length: Int) = {
    inputBuffer.put(data, offset, length)
    inputBuffer.flip()

    val decodeRes = decoder.decode(inputBuffer, outputBuffer, false)
    if (decodeRes.isError) decodeRes.throwException()

    inputBuffer.compact()
    outputBuffer.flip()
    val res = new String(outputBuffer.array(), 0, outputBuffer.remaining())
    outputBuffer.clear()
    res
  }

}
