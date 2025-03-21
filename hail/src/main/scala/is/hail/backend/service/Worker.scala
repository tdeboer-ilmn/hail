package is.hail.backend.service

import java.io._
import java.nio.charset._
import java.util.{concurrent => javaConcurrent}

import is.hail.asm4s._
import is.hail.{HAIL_REVISION, HailContext}
import is.hail.backend.HailTaskContext
import is.hail.io.fs._
import is.hail.services._
import is.hail.utils._
import org.apache.commons.io.IOUtils
import org.apache.log4j.Logger

import scala.collection.mutable
import scala.concurrent.duration.{Duration, MILLISECONDS}
import scala.concurrent.{Future, Await, ExecutionContext}

class ServiceTaskContext(val partitionId: Int) extends HailTaskContext {
  override def stageId(): Int = 0

  override def attemptNumber(): Int = 0
}

object WorkerTimer {
  private val log = Logger.getLogger(getClass.getName())
}

class WorkerTimer() {
  import WorkerTimer._

  var startTimes: mutable.Map[String, Long] = mutable.Map()
  def start(label: String): Unit = {
    startTimes.put(label, System.nanoTime())
  }

  def end(label: String): Unit = {
    val endTime = System.nanoTime()
    val startTime = startTimes.get(label)
    startTime.foreach { s =>
      val durationMS = "%.6f".format((endTime - s).toDouble / 1000000.0)
      log.info(s"$label took $durationMS ms.")
    }
  }
}

object Worker {
  private[this] val log = Logger.getLogger(getClass.getName())
  private[this] val myRevision = HAIL_REVISION
  private[this] implicit val ec = ExecutionContext.fromExecutorService(
    javaConcurrent.Executors.newCachedThreadPool())

  private[this] def writeString(out: DataOutputStream, s: String): Unit = {
    val bytes = s.getBytes(StandardCharsets.UTF_8)
    out.writeInt(bytes.length)
    out.write(bytes)
  }

  def main(argv: Array[String]): Unit = {
    val theHailClassLoader = new HailClassLoader(getClass().getClassLoader())

    if (argv.length != 7) {
      throw new IllegalArgumentException(s"expected seven arguments, not: ${ argv.length }")
    }
    val scratchDir = argv(0)
    val logFile = argv(1)
    var jarLocation = argv(2)
    val kind = argv(3)
    assert(kind == Main.WORKER)
    val root = argv(4)
    val i = argv(5).toInt
    val n = argv(6).toInt
    val timer = new WorkerTimer()

    val deployConfig = DeployConfig.fromConfigFile(
      s"$scratchDir/secrets/deploy-config/deploy-config.json")
    DeployConfig.set(deployConfig)
    val userTokens = Tokens.fromFile(s"$scratchDir/secrets/user-tokens/tokens.json")
    Tokens.set(userTokens)
    tls.setSSLConfigFromDir(s"$scratchDir/secrets/ssl-config")

    log.info(s"is.hail.backend.service.Worker $myRevision")
    log.info(s"running job $i/$n at root $root with scratch directory '$scratchDir'")

    timer.start(s"Job $i/$n")

    timer.start("readInputs")
    val fs = FS.cloudSpecificCacheableFS(s"$scratchDir/secrets/gsa-key/key.json", None)

    // FIXME: HACK
    val (open, create) = if (n <= 50) {
      (fs.openCachedNoCompression _, fs.createCachedNoCompression _)
    } else {
      ((x: String) => fs.openNoCompression(x), fs.createNoCompression _)
    }

    val fFuture = Future {
      retryTransientErrors {
        using(new ObjectInputStream(open(s"$root/f"))) { is =>
          is.readObject().asInstanceOf[(Array[Byte], HailTaskContext, HailClassLoader, FS) => Array[Byte]]
        }
      }
    }

    val contextFuture = Future {
      retryTransientErrors {
        using(open(s"$root/contexts")) { is =>
          is.seek(i * 12)
          val offset = is.readLong()
          val length = is.readInt()
          is.seek(offset)
          val context = new Array[Byte](length)
          is.readFully(context)
          context
        }
      }
    }

    val f = Await.result(fFuture, Duration.Inf)
    val context = Await.result(contextFuture, Duration.Inf)

    timer.end("readInputs")
    timer.start("executeFunction")

    if (HailContext.isInitialized) {
      HailContext.get.backend = new ServiceBackend(null, null, new HailClassLoader(getClass().getClassLoader()), null, None)
    } else {
      HailContext(
        // FIXME: workers should not have backends, but some things do need hail contexts
        new ServiceBackend(null, null, new HailClassLoader(getClass().getClassLoader()), null, None), skipLoggingConfiguration = true, quiet = true)
    }
    val htc = new ServiceTaskContext(i)
    var result: Array[Byte] = null
    var userError: HailException = null
    try {
      retryTransientErrors {
        result = f(context, htc, theHailClassLoader, fs)
      }
    } catch {
      case err: HailException => userError = err
    }
    htc.finish()

    timer.end("executeFunction")
    timer.start("writeOutputs")

    using(create(s"$root/result.$i")) { os =>
      val dos = new DataOutputStream(os)
      if (result != null) {
        assert(userError == null)

        dos.writeBoolean(true)
        dos.write(result)
      } else {
        assert(userError != null)
        val (shortMessage, expandedMessage, errorId) = handleForPython(userError)

        dos.writeBoolean(false)
        writeString(dos, shortMessage)
        writeString(dos, expandedMessage)
        dos.writeInt(errorId)
      }
    }
    timer.end("writeOutputs")
    timer.end(s"Job $i")
    log.info(s"finished job $i at root $root")
  }
}
