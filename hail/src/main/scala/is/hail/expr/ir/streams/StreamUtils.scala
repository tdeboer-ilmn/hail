package is.hail.expr.ir.streams

import is.hail.annotations.Region
import is.hail.asm4s._
import is.hail.expr.ir.{EmitCodeBuilder, IEmitCode, IR, NDArrayMap, NDArrayMap2, Ref, RunAggScan, StagedArrayBuilder, StreamFilter, StreamFlatMap, StreamFold, StreamFold2, StreamFor, StreamJoinRightDistinct, StreamMap, StreamScan, StreamZip, StreamZipJoin}
import is.hail.types.physical.PCanonicalArray
import is.hail.types.physical.stypes.SingleCodeType
import is.hail.types.physical.stypes.interfaces.SIndexableValue
import is.hail.utils.HailException

object StreamUtils {

  def storeNDArrayElementsAtAddress(
    cb: EmitCodeBuilder,
    stream: StreamProducer,
    destRegion: Value[Region],
    addr: Value[Long],
    errorId: Int
  ): Unit = {
    val currentElementIndex = cb.newLocal[Long]("store_ndarray_elements_stream_current_index", 0)
    val currentElementAddress = cb.newLocal[Long]("store_ndarray_elements_stream_current_addr", addr)
    val elementType = stream.element.emitType.storageType
    val elementByteSize = elementType.byteSize

    var push: (EmitCodeBuilder, IEmitCode) => Unit = null
    stream.memoryManagedConsume(destRegion, cb, setup = { cb =>
      push = { case (cb, iec) =>
        iec.consume(cb,
          cb._throw(Code.newInstance[HailException, String, Int](
            "Cannot construct an ndarray with missing values.", errorId
          )),
          { sc =>
            elementType.storeAtAddress(cb, currentElementAddress, destRegion, sc, deepCopy = true)
          })
        cb.assign(currentElementIndex, currentElementIndex + 1)
        cb.assign(currentElementAddress, currentElementAddress + elementByteSize)
      }
    }) { cb =>
      push(cb, stream.element.toI(cb))
    }
  }

  def toArray(
    cb: EmitCodeBuilder,
    stream: StreamProducer,
    destRegion: Value[Region]
  ): SIndexableValue = {
    val mb = cb.emb

    val xLen = mb.newLocal[Int]("sta_len")
    val aTyp = PCanonicalArray(stream.element.emitType.storageType, true)
    stream.length match {
      case None =>
        val vab = new StagedArrayBuilder(SingleCodeType.fromSType(stream.element.st), stream.element.required, mb, 0)
        writeToArrayBuilder(cb, stream, vab, destRegion)
        cb.assign(xLen, vab.size)

        aTyp.constructFromElements(cb, destRegion, xLen, deepCopy = false) { (cb, i) =>
          vab.loadFromIndex(cb, destRegion, i)
        }

      case Some(computeLen) =>

        var pushElem: (EmitCodeBuilder, IEmitCode) => Unit = null
        var finish: (EmitCodeBuilder) => SIndexableValue = null

        stream.memoryManagedConsume(destRegion, cb, setup = { cb =>
          cb.assign(xLen, computeLen(cb))
          val (_pushElem, _finish) = aTyp.constructFromFunctions(cb, destRegion, xLen, deepCopy = stream.requiresMemoryManagementPerElement)
          pushElem = _pushElem
          finish = _finish
        }) { cb =>
          pushElem(cb, stream.element.toI(cb))
        }

        finish(cb)
    }
  }

  def writeToArrayBuilder(
    cb: EmitCodeBuilder,
    stream: StreamProducer,
    ab: StagedArrayBuilder,
    destRegion: Value[Region]
  ): Unit = {
    stream.memoryManagedConsume(destRegion, cb, setup = { cb =>
      cb += ab.clear
      stream.length match {
        case Some(computeLen) => cb += ab.ensureCapacity(computeLen(cb))
        case None => cb += ab.ensureCapacity(16)
      }


    }) { cb =>
      stream.element.toI(cb).consume(cb,
        cb += ab.addMissing(),
        sc => cb += ab.add(ab.elt.coerceSCode(cb, sc, destRegion, deepCopy = stream.requiresMemoryManagementPerElement).code)
      )
    }
  }

  private[ir] def multiplicity(root: IR, refName: String): Int = {
    var uses = 0

    // assumes no name collisions, a bit hacky...
    def traverse(ir: IR, mult: Int): Unit = ir match {
      case Ref(name, _) => if (refName == name) uses += mult
      case StreamMap(a, _, b) => traverse(a, mult); traverse(b, 2)
      case StreamFilter(a, _, b) => traverse(a, mult); traverse(b, 2)
      case StreamFlatMap(a, _, b) => traverse(a, mult); traverse(b, 2)
      case StreamJoinRightDistinct(l, r, _, _, _, c, j, _) =>
        traverse(l, mult); traverse(r, mult); traverse(j, 2)
      case StreamScan(a, z, _, _, b) =>
        traverse(a, mult); traverse(z, 2); traverse(b, 2)
      case RunAggScan(a, _, i, s, r, _) =>
        traverse(a, mult); traverse(i, 2); traverse(s, 2); traverse(r, 2)
      case StreamZipJoin(as, _, _, _, f) =>
        as.foreach(traverse(_, mult)); traverse(f, 2)
      case StreamZip(as, _, body, _, _) =>
        as.foreach(traverse(_, mult)); traverse(body, 2)
      case StreamFold(a, zero, _, _, body) =>
        traverse(a, mult); traverse(zero, mult); traverse(body, 2)
      case StreamFold2(a, accs, _, seqs, res) =>
        traverse(a, mult)
        accs.foreach { case (_, acc) => traverse(acc, mult) }
        seqs.foreach(traverse(_, 2))
        traverse(res, 2)
      case StreamFor(a, _, body) =>
        traverse(a, mult); traverse(body, 2)
      case NDArrayMap(a, _, body) =>
        traverse(a, mult); traverse(body, 2)
      case NDArrayMap2(l, r, _, _, body, _) =>
        traverse(l, mult); traverse(r, mult); traverse(body, 2)

      case _ => ir.children.foreach {
        case child: IR => traverse(child, mult)
        case _ =>
      }
    }

    traverse(root, 1)
    uses min 2
  }

  def isIterationLinear(ir: IR, refName: String): Boolean =
    multiplicity(ir, refName) <= 1
}
