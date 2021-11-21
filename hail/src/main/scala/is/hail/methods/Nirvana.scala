package is.hail.methods

import com.fasterxml.jackson.core.JsonParseException
import is.hail.annotations._
import is.hail.backend.ExecuteContext
import is.hail.expr._
import is.hail.expr.ir.functions.{RelationalFunctions, TableToTableFunction}
import is.hail.expr.ir.TableValue
import is.hail.io.fs.FS
import is.hail.methods.Nirvana._
import is.hail.rvd.RVD
import is.hail.sparkextras.ContextRDD
import is.hail.types._
import is.hail.types.physical.PType
import is.hail.types.virtual._
import is.hail.utils._
import is.hail.variant.{Locus, RegionValueVariant, VariantMethods}
import org.apache.spark.sql.Row
import org.json4s.jackson.JsonMethods
import org.json4s.{Formats, JValue}

import scala.collection.JavaConverters._
import scala.collection.mutable

case class NirvanaConfiguration(
  command: Array[String],
  env: Map[String, String],
  nirvana_json_schema: TStruct)

object Nirvana {

  def readConfiguration(fs: FS, path: String): NirvanaConfiguration = {
    val jv = using(fs.open(path)) { in =>
      JsonMethods.parse(in)
    }
    implicit val formats: Formats = defaultJSONFormats + new TStructSerializer
    jv.extract[NirvanaConfiguration]
  }

  def printContext(w: (String) => Unit) {
    w("##fileformat=VCFv4.1")
    w("#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO")
  }

  def printElement(w: (String) => Unit, v: (Locus, IndexedSeq[String])) {
    val (locus, alleles) = v

    val sb = new StringBuilder()
    sb.append(locus.contig)
    sb += '\t'
    sb.append(locus.position)
    sb.append("\t.\t")
    sb.append(alleles(0))
    sb += '\t'
    sb.append(alleles.tail.filter(_ != "*").mkString(","))
    sb.append("\t.\t.")
    w(sb.result())
  }

  def variantFromInput(input: String): (Locus, IndexedSeq[String]) = {
    info(input)
    try {
      val a = input.split("\t")
      (Locus(a(0), a(1).toInt), a(3) +: a(4).split(","))
    } catch {
      case e: Throwable => fatal(s"Nirvana returned invalid variant '$input'", e)
    }
  }

  def waitFor(proc: Process, err: StringBuilder, cmd: Array[String]): Unit = {
    val rc = proc.waitFor()

    if (rc != 0) {
      fatal(s"Nirvana command '${ cmd.mkString(" ") }' failed with non-zero exit status $rc\n" +
        "  Nirvana Error output:\n" + err.toString)
    }
  }

  def apply(fs: FS, params: NirvanaParameters): Nirvana = {
    val conf = Nirvana.readConfiguration(fs, params.config)
    new Nirvana(params, conf)
  }

  def apply(fs: FS, config: String, blockSize: Int, tolerateParseError: Boolean): Nirvana =
    Nirvana(fs, NirvanaParameters(config, blockSize, tolerateParseError))

  def fromJValue(fs: FS, jv: JValue): Nirvana = {
    log.info(s"nirvana config json: ${ jv.toString }")
    implicit val formats: Formats = RelationalFunctions.formats
    val params = jv.extract[NirvanaParameters]
    Nirvana(fs, params)
  }
}

case class NirvanaParameters(config: String, blockSize: Int, tolerateParseError: Boolean)

class Nirvana(val params: NirvanaParameters, conf: NirvanaConfiguration) extends TableToTableFunction {
  //info("Running Nirvana")
  //Default structure for Nirvana (valid for 3.17+)
  private def nirvanaSignature =  TStruct(
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
          )),
        "genes" -> TArray(TStruct(
          "name" -> TString,
          "hgncId" -> TInt32,
          "summary" -> TString,
          "omim" -> TArray(TStruct(
            "mimNumber" -> TInt32,
            "geneName" -> TString,
            "description" -> TString,
            "phenotypes" -> TArray(TStruct(
              "mimNumber" -> TInt32,
              "phenotype" -> TString,
              "description" -> TString,
              "mapping" -> TString,
              "inheritance" -> TArray(TString),
              "comments" -> TArray(TString)
            ))
          )),
          "gnomAD" -> TStruct(
            "pLi" -> TFloat64,
            "pRec" -> TFloat64,
            "pNull" -> TFloat64,
            "synZ" -> TFloat64,
            "misZ" -> TFloat64,
            "loeuf" -> TFloat64
          ),
          "clingenGeneValidity" -> TArray(TStruct(
            "diseaseId" -> TString,
            "disease" -> TString,
            "classification" -> TString,
            "classificationDate" -> TString
          ))
        ))
      )

  //private def nirvanaSignature = conf.nirvana_json_schema

  override def preservesPartitionCounts: Boolean = false

  override def typ(childType: TableType): TableType = {
    val nirvanaType = nirvanaSignature
    TableType(childType.rowType ++ TStruct("nirvana" -> nirvanaType), childType.key, childType.globalType)
  }

  override def execute(ctx: ExecuteContext, tv: TableValue): TableValue = {
    assert(tv.typ.key == FastIndexedSeq("locus", "alleles"))
    assert(tv.typ.rowType.size == 2)
    info(s"Running Nirvana")

    val localConf = conf
    val localNirvanaSignature = nirvanaSignature

    val cmd = localConf.command
    info(cmd.mkString(" "))

    val contigQuery: Querier = nirvanaSignature.query("chromosome")
    val startQuery = nirvanaSignature.query("position")
    val refQuery = nirvanaSignature.query("refAllele")
    val altsQuery = nirvanaSignature.query("altAlleles")

    val localBlockSize = params.blockSize

    val localRowType = tv.rvd.rowPType
    val localTolerateParseError = params.tolerateParseError

    val prev = tv.rvd
    val annotations = prev
      .mapPartitionsWithIndex { (partIdx, _, it) =>
        val pb = new ProcessBuilder(cmd.toList.asJava)
        val env = pb.environment()
        localConf.env.foreach { case (key, value) =>
          env.put(key, value)
        }
        val warnContext = new mutable.HashSet[String]

        val rvv = new RegionValueVariant(localRowType)
        it
          .map { ptr =>
            rvv.set(ptr)
            (rvv.locus(), rvv.alleles(): IndexedSeq[String])
          }
          .grouped(localBlockSize)
          .zipWithIndex
          .flatMap { case (block, blockIdx) =>
            val procID = Annotation(partIdx, blockIdx)
            val (jt, err, proc) = block.iterator.pipe(pb,
              printContext,
              printElement,
              _ => ())

            val nonStarToOriginalVariant = block.map { case v@(locus, alleles) =>
              (locus, alleles.filter(_ != "*")) -> v
            }.toMap

            val kt: Map[Annotation, Annotation] = jt
              .filter(_.startsWith("{\"chromosome"))
              .flatMap { s =>
                try {
                  val jv = JsonMethods.parse(s)
                  val a = JSONAnnotationImpex.importAnnotation(jv, localNirvanaSignature, warnContext = warnContext)
                  val (locusQ, posQ, refQ, altsQ) = (contigQuery(a),startQuery(a),refQuery(a),altsQuery(a))
                  if (locusQ == null || posQ == null || refQ == null || altsQ == null)
                    fatal(s"Nirvana generated null variant string" +
                      s"\n  json:   $s" +
                      s"\n  parsed: $a")

                  val nirvanaLocus = Locus(contigQuery(a).asInstanceOf[String], startQuery(a).asInstanceOf[Int])
                  val nirvanaAlleles = refQuery(a).asInstanceOf[String] +: altsQuery(a).asInstanceOf[IndexedSeq[String]]

                  nonStarToOriginalVariant.get((nirvanaLocus, nirvanaAlleles)) match {
                    case Some(v@(locus, alleles)) =>
                      Some((Annotation(locus, alleles), a))
                    case None =>
                      fatal(s"Nirvana output variant ${ VariantMethods.locusAllelesToString(nirvanaLocus, nirvanaAlleles) } not found in original variants.\nNirvana output: $s")
                  }
                } catch {
                  case e: JsonParseException if localTolerateParseError =>
                    log.warn(s"Nirvana failed to produce parsable JSON!\n  json: $s\n  error: $e")
                    None
                }
              }.toMap

            waitFor(proc, err, cmd)

            block.map { case (locus, alleles) =>
              val variant = Annotation(locus, alleles)
              val nirvanaAnnotation = kt.get(variant).orNull
              (variant, nirvanaAnnotation, procID)
            }
          }
      }

    val nirvanaType: Type = nirvanaSignature

    val nirvanaRVDType = prev.typ.copy(rowType = prev.rowPType
      .appendKey("nirvana", PType.canonical(nirvanaType))
      .appendKey("nirvana_proc_id", PType.canonical(TStruct("part_idx" -> TInt32, "block_idx" -> TInt32))))

    val nirvanaRowType = nirvanaRVDType.rowType

    val nirvanaRVD: RVD = RVD(
      nirvanaRVDType,
      prev.partitioner,
      ContextRDD.weaken(annotations).cmapPartitions { (ctx, it) =>
        val rvb = ctx.rvb

        it.map { case (v, nirvana, proc) =>
          rvb.start(nirvanaRowType)
          rvb.startStruct()
          rvb.addAnnotation(nirvanaRowType.types(0).virtualType, v.asInstanceOf[Row].get(0))
          rvb.addAnnotation(nirvanaRowType.types(1).virtualType, v.asInstanceOf[Row].get(1))
          rvb.addAnnotation(nirvanaRowType.types(2).virtualType, nirvana)
          rvb.addAnnotation(nirvanaRowType.types(3).virtualType, proc)
          rvb.endStruct()

          rvb.end()
        }
      })

    val (globalValue, globalType) =
      (Row(), TStruct.empty)

    TableValue(ctx,
      TableType(nirvanaRowType.virtualType, FastIndexedSeq("locus", "alleles"), globalType),
      BroadcastRow(ctx, globalValue, globalType),
      nirvanaRVD)
  }

  override def toJValue: JValue = {
    decomposeWithName(params, "Nirvana")(RelationalFunctions.formats)
  }

  override def hashCode(): Int = params.hashCode()

  override def equals(that: Any): Boolean = that match {
    case that: Nirvana => params == that.params
    case _ => false
  }
}
