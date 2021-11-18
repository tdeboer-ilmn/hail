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
//Added for parsing the nirvana struct to one that Nirvana.scala uses
import is.hail.expr.ir.IRParser

import scala.collection.JavaConverters._
import scala.collection.mutable


object Nirvana {

  //For Nirnava v3.17.0

  val nirvanaSignature = TStruct(
        "chromosome" -> TString,
        "position" -> TInt32,
        "repeatUnit" -> TString,
        "refRepeatCount" -> TInt32,
        "svEnd" -> TInt32,
        "refAllele" -> TString,
        "altAlleles" -> TArray(TString),
        "quality" -> TFloat64,
        "filters" -> TArray(TString),
        "ciPos" -> TArray(TInt32),
        "ciEnd" -> TArray(TInt32),
        "svLength" -> TInt32,
        "strandBias" -> TFloat64,
        "jointSomaticNormalQuality" -> TInt32,
        "cytogeneticBand" -> TString,
        "clingen" -> TArray(TStruct(
            "chromosome" -> TString,
            "begin" -> TInt32,
            "end" -> TInt32,
            "variantType" -> TString,
            "id" -> TString,
            "clinicalInterpretation" -> TString,
            "observedGains" -> TInt32,
            "observedLosses" -> TInt32,
            "validated" -> TBoolean,
            "phenotypes" -> TArray(TString),
            "phenotypeIds" -> TArray(TString),
            "reciprocalOverlap" -> TFloat64
        )),
        "clingenDosageSensitivityMap" -> TArray(TStruct(
            "chromosome" -> TString,
            "begin" -> TInt32,
            "end" -> TInt32,
            "haploinsufficiency" -> TString,
            "triplosensitivity" -> TString,
            "reciprocalOverlap" -> TFloat64,
            "annotationOverlap" -> TFloat64
        )),
        "oneKg" -> TArray(TStruct(
            "chromosome" -> TString,
            "begin" -> TInt32,
            "end" -> TInt32,
            "variantType" -> TString,
            "id" -> TString,
            "allAn" -> TFloat64,
            "allAc" -> TFloat64,
            "allAf" -> TFloat64,
            "afrAf" -> TFloat64,
            "amrAf" -> TFloat64,
            "eurAf" -> TFloat64,
            "easAf" -> TInt32,
            "sasAf" -> TInt32,
            "reciprocalOverlap" -> TString
        )),
        "mitomap" -> TArray(TStruct(
            "chromosome" -> TString,
            "begin" -> TInt32,
            "end" -> TInt32,
            "variantType" -> TArray(TString),
            "reciprocalOverlap" -> TFloat64,
            "annotationOverlap" -> TFloat64
        )),
        "variants" -> TArray(TStruct(
            "vid" -> TString,
            "chromosome" -> TString,
            "begin" -> TInt32,
            "end" -> TInt32,
            "isReferenceMinorAllele" -> TBoolean,
            "isStructuralVariant" -> TBoolean,
            "inLowComplexityRegion" -> TBoolean,
            "refAllele" -> TString,
            "altAllele" -> TString,
            "variantType" -> TString,
            "isDecomposedVariant" -> TBoolean,
            "isRecomposedVariant" -> TBoolean,
            "linkedVids" -> TArray(TString),
            "hgvsg" -> TString,
            "phylopScore" -> TFloat64,
            "globalAllele" -> TStruct(
                "globalMinorAllele" -> TString,
                "globalMinorAlleleFrequency" -> TFloat64
            ),
            "transcripts" -> TArray(TStruct(
                "transcript" -> TString,
                "source" -> TString,
                "bioType" -> TString,
                "codons" -> TString,
                "aminoAcids" -> TString,
                "cdnaPos" -> TString,
                "cdsPos" -> TString,
                "exons" -> TString,
                "introns" -> TString,
                "proteinPos" -> TString,
                "geneId" -> TString,
                "hgnc" -> TString,
                "consequence" -> TArray(TString),
                "hgvsc" -> TString,
                "hgvsp" -> TString,
                "geneFusion" -> TStruct(
                    "exon" -> TInt32,
                    "intron" -> TInt32,
                    "fusions" -> TArray(TStruct(
                        "hgvsc" -> TString,
                        "exon" -> TInt32,
                        "intron" -> TInt32
                    ))
                ),
                "isCanonical" -> TBoolean,
                "polyPhenScore" -> TFloat64,
                "polyPhenPrediction" -> TString,
                "proteinId" -> TString,
                "siftScore" -> TFloat64,
                "siftPrediction" -> TString,
                "completeOverlap" -> TBoolean,
                "aminoAcidConservation" -> TStruct(
                    "scores" -> TArray(TFloat64)
                )
            )),
            "regulatoryRegions" -> TArray(TStruct(
                "id" -> TString,
                "type" -> TString,
                "consequence" -> TArray(TString)
            )),
            "clinvar" -> TArray(TStruct(
                "id" -> TString,
                "variationId" -> TString,
                "reviewStatus" -> TString,
                "alleleOrigins" -> TArray(TString),
                "refAllele" -> TString,
                "altAllele" -> TString,
                "phenotypes" -> TArray(TString),
                "medGenIds" -> TArray(TString),
                "omimIds" -> TArray(TString),
                "orphanetIds" -> TArray(TString),
                "significance" -> TArray(TString),
                "lastUpdatedDate" -> TString,
                "pubMedIds" -> TArray(TString),
                "isAlleleSpecific" -> TBoolean
            )),
            "oneKg" -> TStruct(
                "allAf" -> TFloat64,
                "allAc" -> TInt32,
                "allAn" -> TInt32,
                "afrAf" -> TFloat64,
                "afrAc" -> TInt32,
                "afrAn" -> TInt32,
                "amrAf" -> TFloat64,
                "amrAc" -> TInt32,
                "amrAn" -> TInt32,
                "easAf" -> TFloat64,
                "easAc" -> TInt32,
                "easAn" -> TInt32,
                "eurAf" -> TFloat64,
                "eurAc" -> TInt32,
                "eurAn" -> TInt32,
                "sasAf" -> TFloat64,
                "sasAc" -> TInt32,
                "sasAn" -> TInt32
            ),
            "gnomad" -> TStruct(
                "coverage" -> TInt32,
                "allAf" -> TFloat64,
                "maleAf" -> TFloat64,
                "femaleAf" -> TFloat64,
                "controlsAllAf" -> TFloat64,
                "allAc" -> TInt32,
                "maleAc" -> TInt32,
                "femaleAc" -> TInt32,
                "controlsAllAc" -> TInt32,
                "allAn" -> TInt32,
                "maleAn" -> TInt32,
                "femaleAn" -> TInt32,
                "controlsAllAn" -> TInt32,
                "allHc" -> TInt32,
                "maleHc" -> TInt32,
                "femaleHc" -> TInt32,
                "afrAf" -> TFloat64,
                "afrAc" -> TInt32,
                "afrAn" -> TInt32,
                "afrHc" -> TInt32,
                "amrAf" -> TFloat64,
                "amrAc" -> TInt32,
                "amrAn" -> TInt32,
                "amrHc" -> TInt32,
                "easAf" -> TFloat64,
                "easAc" -> TInt32,
                "easAn" -> TInt32,
                "easHc" -> TInt32,
                "finAf" -> TFloat64,
                "finAc" -> TInt32,
                "finAn" -> TInt32,
                "finHc" -> TInt32,
                "nfeAf" -> TFloat64,
                "nfeAc" -> TInt32,
                "nfeAn" -> TInt32,
                "nfeHc" -> TInt32,
                "othAf" -> TFloat64,
                "othAc" -> TInt32,
                "othAn" -> TInt32,
                "othHc" -> TInt32,
                "asjAf" -> TFloat64,
                "asjAc" -> TInt32,
                "asjAn" -> TInt32,
                "asjHc" -> TInt32,
                "sasAf" -> TFloat64,
                "sasAc" -> TInt32,
                "sasAn" -> TInt32,
                "sasHc" -> TInt32,
                "failedFilter" -> TBoolean,
                "lowComplexityRegion" -> TBoolean
            ),
            "dbsnp" -> TArray(TString),
            "mitomap" -> TArray(TStruct(
                "refAllele" -> TString,
                "altAllele" -> TString,
                "diseases" -> TArray(TString),
                "hasHomoplasmy" -> TBoolean,
                "hasHeteroplasmy" -> TBoolean,
                "status" -> TString,
                "clinicalSignificance" -> TString,
                "scorePercentile" -> TFloat64,
                "numGenBankFullLengthSeqs" -> TInt32,
                "pubMedIds" -> TArray(TString),
                "isAlleleSpecific" -> TBoolean
            )),
            "primateAI" -> TArray(TStruct(
                "hgnc" -> TString,
                "scorePercentile" -> TFloat64
            )),
            "revel" -> TStruct(
                "score" -> TFloat64
            ),
            "spliceAI" -> TArray(TStruct(
                "hgnc" -> TString,
                "acceptorGainDistance" -> TInt32,
                "acceptorGainScore" -> TFloat64,
                "acceptorLossDistance" -> TInt32,
                "acceptorLossScore" -> TFloat64,
                "donorGainDistance" -> TInt32,
                "donorGainScore" -> TFloat64,
                "donorLossDistance" -> TInt32,
                "donorLossScore" -> TFloat64
            )),
            "topmed" -> TStruct(
                "allAc" -> TInt32,
                "allAn" -> TInt32,
                "allAf" -> TFloat64,
                "allHc" -> TInt32,
                "failedFilter" -> TBoolean
            )
        ))
    )

  def printContext(w: (String) => Unit) {
    w("##fileformat=VCFv4.2")
    w("#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO") //Remove FORMAT since Nirvana would them expect genotypes to be provided as well
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
    sb.append("\t.\t.") //Removed GT
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

            // The filter is because every other output line is a comma.
            val kt = jt.filter(_.startsWith("{\"chromosome")).map { s =>
              val a = JSONAnnotationImpex.importAnnotation(JsonMethods.parse(s), nirvanaSignature, warnContext = warnContext)
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
