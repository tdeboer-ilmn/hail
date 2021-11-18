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
import is.hail.expr.ir.IRParser.parseStructType

import scala.collection.JavaConverters._
import scala.collection.mutable


object Nirvana {

  //For Nirnava v3.17.0

  /*val nirvanaParseableStruct = """struct{
          |chromosome: str,
          |position: int,
          |refAllele: str,
          |altAlleles: array<str>,
          |quality: float,
          |filters: array<str>,
          |cytogeneticBand: str
        |}""".stripMargin.replaceAll("\n","")

  val nirvanaParseableStruct = """struct{
        chromosome: str,
        position: int,
        repeatUnit: str,
        refRepeatCount: int,
        svEnd: int,
        refAllele: str,
        altAlleles: array<str>,
        quality: float,
        filters: array<str>,
        ciPos: array<int>,
        ciEnd: array<int>,
        svLength: int,
        strandBias: float,
        jointSomaticNormalQuality: int,
        cytogeneticBand: str,
        clingen: array<struct{
            chromosome: str,
            begin: int,
            end: int,
            variantType: str,
            id: str,
            clinicalInterpretation: str,
            observedGains: int,
            observedLosses: int,
            validated: bool,
            phenotypes: array<str>,
            phenotypeIds: array<str>,
            reciprocalOverlap: float
        }>,
        clingenDosageSensitivityMap: array<struct{
            chromosome: str,
            begin: int,
            end: int,
            haploinsufficiency: str,
            triplosensitivity: str,
            reciprocalOverlap: float,
            annotationOverlap: float
        }>,
        oneKg: array<struct{
            chromosome: str,
            begin: int,
            end: int,
            variantType: str,
            id: str,
            allAn: float,
            allAc: float,
            allAf: float,
            afrAf: float,
            amrAf: float,
            eurAf: float,
            easAf: int,
            sasAf: int,
            reciprocalOverlap: str
        }>,
        mitomap: array<struct{
            chromosome: str,
            begin: int,
            end: int,
            variantType: array<str>,
            reciprocalOverlap: float,
            annotationOverlap: float
        }>,
        variants: array<struct{
            vid: str,
            chromosome: str,
            begin: int,
            end: int,
            isReferenceMinorAllele: bool,
            isStructuralVariant: bool,
            inLowComplexityRegion: bool,
            refAllele: str,
            altAllele: str,
            variantType: str,
            isDecomposedVariant: bool,
            isRecomposedVariant: bool,
            linkedVids: array<str>,
            hgvsg: str,
            phylopScore: float,
            globalAllele:struct{
                globalMinorAllele:str,
                globalMinorAlleleFrequency:float
            },
            transcripts: array<struct{
                transcript: str,
                source: str,
                bioType: str,
                codons: str,
                aminoAcids: str,
                cdnaPos: str,
                cdsPos: str,
                exons: str,
                introns: str,
                proteinPos: str,
                geneId: str,
                hgnc: str,
                consequence: array<str>,
                hgvsc: str,
                hgvsp: str,
                geneFusion: struct{
                    exon:int,
                    intron: int,
                    fusions: array<struct{
                        hgvsc:str,
                        exon:int,
                        intron:int
                    }>
                },
                isCanonical: bool,
                polyPhenScore: float,
                polyPhenPrediction: str,
                proteinId: str,
                siftScore: float,
                siftPrediction: str,
                completeOverlap: bool,
                aminoAcidConservation: struct{
                    scores: array<float>
                }
            }>,
            regulatoryRegions:array<struct{
                id: str,
                type: str,
                consequence: array<str>
            }>,
            clinvar:array<struct{
                id: str,
                variationId: str,
                reviewStatus: str,
                alleleOrigins: array<str>,
                refAllele: str,
                altAllele: str,
                phenotypes: array<str>,
                medGenIds: array<str>,
                omimIds: array<str>,
                orphanetIds: array<str>,
                significance: array<str>,
                lastUpdatedDate: str,
                pubMedIds: array<str>,
                isAlleleSpecific: bool
            }>,
            oneKg:struct{
                allAf: float,
                allAc: int,
                allAn: int,
                afrAf: float,
                afrAc: int,
                afrAn: int,
                amrAf: float,
                amrAc: int,
                amrAn: int,
                easAf: float,
                easAc: int,
                easAn: int,
                eurAf: float,
                eurAc: int,
                eurAn: int,
                sasAf: float,
                sasAc: int,
                sasAn: int
            },
            gnomad:struct{
                coverage: int,
                allAf: float,
                maleAf: float,
                femaleAf: float,
                controlsAllAf: float,
                allAc: int,
                maleAc: int,
                femaleAc: int,
                controlsAllAc: int,
                allAn: int,
                maleAn: int,
                femaleAn: int,
                controlsAllAn: int,
                allHc: int,
                maleHc: int,
                femaleHc: int,
                afrAf: float,
                afrAc: int,
                afrAn: int,
                afrHc: int,
                amrAf: float,
                amrAc: int,
                amrAn: int,
                amrHc: int,
                easAf: float,
                easAc: int,
                easAn: int,
                easHc: int,
                finAf: float,
                finAc: int,
                finAn: int,
                finHc: int,
                nfeAf: float,
                nfeAc: int,
                nfeAn: int,
                nfeHc: int,
                othAf: float,
                othAc: int,
                othAn: int,
                othHc: int,
                asjAf: float,
                asjAc: int,
                asjAn: int,
                asjHc: int,
                sasAf: float,
                sasAc: int,
                sasAn: int,
                sasHc: int,
                failedFilter: bool,
                lowComplexityRegion: bool
            },
            dbsnp:array<str>,
            mitomap:array<struct{
                refAllele: str,
                altAllele: str,
                diseases: array<str>,
                hasHomoplasmy: bool,
                hasHeteroplasmy: bool,
                status: str,
                clinicalSignificance: str,
                scorePercentile: float,
                numGenBankFullLengthSeqs: int,
                pubMedIds: array<str>,
                isAlleleSpecific: bool
            }>,
            primateAI:array<struct{
                hgnc: str,
                scorePercentile: float
            }>,
            revel:struct{
                score: float
            },
            spliceAI:array<struct{
                hgnc: str,
                acceptorGainDistance: int,
                acceptorGainScore: float,
                acceptorLossDistance: int,
                acceptorLossScore: float,
                donorGainDistance: int,
                donorGainScore: float,
                donorLossDistance: int,
                donorLossScore: float
            }>,
            topmed:struct{
                allAc: int,
                allAn: int,
                allAf: float,
                allHc: int,
                failedFilter: bool
            }
        }>
    }""".stripMargin.replaceAll("\n", "")

  val nirvanaSignature = parseStructType(nirvanaParseableStruct)*/

    val nirvanaSignature = TStruct(
      "chromosome" -> TString,
      "refAllele" -> TString,
      "position" -> TInt32,
      "altAlleles" -> TArray(TString),
      "cytogeneticBand" -> TString,
      "quality" -> TFloat64,
      "filters" -> TArray(TString)
    )

  //DEBUG
  //fatal(s"$nirvanaSignature")

  def printContext(w: (String) => Unit) {
    w("##fileformat=VCFv4.1")
    w("#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO") //Removed GT field since Nirvana does not like that to be there
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
    sb.append("\t.\t.") //Remove GT
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

            //DEBUG
            //jt.next()
            //fatal(jt.next()) //<- This one is empty when both uncommented. But shows top line when by itself.
            //val aap = jt.next()
            //val noot = jt.next()
            //fatal(s"First: $aap" + s"\nSecond: $noot" + s"\nEnd of the line")

            // The filter is because every other output line is a comma.
            val kt = jt.filter(_.startsWith("{\"chromosome")).map { s =>
              val a = JSONAnnotationImpex.importAnnotation(JsonMethods.parse(s), nirvanaSignature, warnContext = warnContext)
              //DEBUG
              //fatal(s"Nirvana generated this" +
              //          s"\n  json:   $s" +
              //          s"\n  parsed: $a")
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
