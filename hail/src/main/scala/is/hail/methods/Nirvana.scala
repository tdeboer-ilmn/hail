package is.hail.methods

import java.io.{FileInputStream, IOException}
import java.util.Properties
import is.hail.annotations._
import is.hail.backend.ExecuteContext
import is.hail.expr.JSONAnnotationImpex
import is.hail.expr.ir.TableValue
import is.hail.expr.ir.functions.TableToTableFunction
import is.hail.types._
import is.hail.types.physical.{PCanonicalStruct, PStruct, PType}
import is.hail.types.virtual._
import is.hail.rvd.{RVD, RVDContext, RVDType}
import is.hail.sparkextras.ContextRDD
import is.hail.utils._
import is.hail.variant.{Locus, RegionValueVariant}
import org.apache.spark.sql.Row
import org.apache.spark.storage.StorageLevel
import org.json4s.jackson.JsonMethods

import scala.collection.JavaConverters._
import scala.collection.mutable


object Nirvana {

  //For Nirnava v3.17.0

  val nirvanaSignature = TStruct(
    "chromosome" -> TString,
    "refAllele" -> TString,
    "position" -> TInt32,
    "altAlleles" -> TArray(TString),
    "cytogeneticBand" -> TString
  )

  def printContext(w: (String) => Unit) {
    w("##fileformat=VCFv4.1")
    w("#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT")
  }

  def printElement(vaSignature: PType)(w: (String) => Unit, v: (Locus, Array[String])) {
    val (locus, alleles) = v

    val sb = new StringBuilder()
    sb.append(locus.contig)
    sb += '\t'
    sb.append(locus.position)
    sb.append("\t.\t")
    sb.append(alleles(0))
    sb += '\t'
    sb.append(alleles.tail.filter(_ != "*").mkString(","))
    sb += '\t'
    sb.append("\t.\t.\tGT")
    w(sb.result())
  }

  def annotate(ctx: ExecuteContext, tv: TableValue, config: String, blockSize: Int): TableValue = {
    assert(tv.typ.key == FastIndexedSeq("locus", "alleles"))
    assert(tv.typ.rowType.size == 2)

    val properties = try {
      val p = new Properties()
      val is = new FileInputStream(config)
      p.load(is)
      is.close()
      p
    } catch {
      case e: IOException =>
        fatal(s"could not open file: ${ e.getMessage }")
    }

    //Additional properties for the wrapper script location
    val wrapper = properties.getProperty("hail.nirvana.wrapper", "run_Nirvana.sh")
      
    val dotnet = properties.getProperty("hail.nirvana.dotnet", "dotnet")

    val nirvanaLocation = properties.getProperty("hail.nirvana.location")
    if (nirvanaLocation == null)
      fatal("property hail.nirvana.location' required")

    val path = Option(properties.getProperty("hail.nirvana.path"))

    val cache = properties.getProperty("hail.nirvana.cache")


    val supplementaryAnnotationDirectoryOpt = Option(properties.getProperty("hail.nirvana.supplementaryAnnotationDirectory"))
    val supplementaryAnnotationDirectory = if (supplementaryAnnotationDirectoryOpt.isEmpty) List[String]() else List("-s", supplementaryAnnotationDirectoryOpt.get)

    val reference = properties.getProperty("hail.nirvana.reference")

    val cmd: List[String] = List[String](wrapper, "-d", dotnet) ++
      List("-c", cache) ++
      supplementaryAnnotationDirectory ++
      List("-r", reference,
           s"$nirvanaLocation"
        )

    println(cmd.mkString(" "))

    val contigQuery: Querier = nirvanaSignature.query("chromosome")
    val startQuery = nirvanaSignature.query("position")
    val refQuery = nirvanaSignature.query("refAllele")
    val altsQuery = nirvanaSignature.query("altAlleles")
    val localRowType = tv.rvd.rowPType
    val localBlockSize = blockSize

    val rowKeyOrd = tv.typ.keyType.ordering

    info("Running Nirvana")

    val prev = tv.rvd
      
    val annotations = prev
      .mapPartitions { (_, it) =>
        val pb = new ProcessBuilder(cmd.asJava)
        val env = pb.environment()
        if (path.orNull != null)
          env.put("PATH", path.get)

        val warnContext = new mutable.HashSet[String]

        val rvv = new RegionValueVariant(localRowType)

        it.map { ptr =>
          rvv.set(ptr)
          (rvv.locus(), rvv.alleles())
        }
          .grouped(localBlockSize)
          .flatMap { block =>
            val (jt, err, proc) = block.iterator.pipe(pb,
              printContext,
              printElement(localRowType),
              _ => ())

            //jt.next()
            //fatal(jt.next()) //<- This one is empty when both uncommented. But shows top line when by itself.
            val aap = jt.next()
            val noot = jt.next()
            fatal(s"First: $aap" + s"\nSecond: $noot" + s"\nEnd of the line")  
            // The filter is because every other output line is a comma.
            val kt = jt.filter(_.startsWith("""{"chromosome""")).map { s => 
              val a = JSONAnnotationImpex.importAnnotation(JsonMethods.parse(s), nirvanaSignature, warnContext = warnContext)
              //This never gets reached, so it seems the iterator is empty after getting the first line?
              fatal(s"Nirvana generated this" +
                        s"\n  json:   $s" +
                        s"\n  parsed: $a")
              val locus = Locus(contigQuery(a).asInstanceOf[String],
                startQuery(a).asInstanceOf[Int])
              val alleles = refQuery(a).asInstanceOf[String] +: altsQuery(a).asInstanceOf[IndexedSeq[String]]
              (Annotation(locus, alleles), a)
            }
              
            val r = kt.toArray
              .sortBy(_._1)(rowKeyOrd.toOrdering)
              
            val rc = proc.waitFor()
            if (rc != 0)
              fatal(s"nirvana command failed with non-zero exit status $rc\n\tError:\n${err.toString}")
            
            r
          }
      }
    
    val nirvanaRVDType = prev.typ.copy(rowType = prev.rowPType.appendKey("nirvana", PType.canonical(nirvanaSignature)))

    val nirvanaRowType = nirvanaRVDType.rowType

    val nirvanaRVD: RVD = RVD(
      nirvanaRVDType,
      prev.partitioner,
      ContextRDD.weaken(annotations).cmapPartitions { (ctx, it) =>
        val rvb = new RegionValueBuilder(ctx.region)

        it.map { case (v, nirvana) =>
          rvb.start(nirvanaRowType)
          rvb.startStruct()
          rvb.addAnnotation(nirvanaRowType.types(0).virtualType, v.asInstanceOf[Row].get(0))
          rvb.addAnnotation(nirvanaRowType.types(1).virtualType, v.asInstanceOf[Row].get(1))
          rvb.addAnnotation(nirvanaRowType.types(2).virtualType, nirvana)
          rvb.endStruct()

          rvb.end()
        }
      }).persist(ctx, StorageLevel.MEMORY_AND_DISK)

      TableValue(ctx,
        TableType(nirvanaRowType.virtualType, FastIndexedSeq("locus", "alleles"), TStruct.empty),
        BroadcastRow.empty(ctx),
        nirvanaRVD
      )
  }
}

case class Nirvana(config: String, blockSize: Int = 500000) extends TableToTableFunction {
  override def typ(childType: TableType): TableType = {
    assert(childType.key == FastIndexedSeq("locus", "alleles"))
    assert(childType.rowType.size == 2)
    TableType(childType.rowType ++ TStruct("nirvana" -> Nirvana.nirvanaSignature), childType.key, childType.globalType)
  }

  def preservesPartitionCounts: Boolean = false

  def execute(ctx: ExecuteContext, tv: TableValue): TableValue = {
    Nirvana.annotate(ctx, tv, config, blockSize)
  }
}
