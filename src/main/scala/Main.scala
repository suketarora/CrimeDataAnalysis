import org.apache.spark._
import scala.math.BigDecimal
import scala.io.Source
import java.sql.Date
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._                   //import Window library



object Main extends App {



 case class CrimeRecord(id: Int, Case_Number: String, Date: String, Block:String, IUCR: String, Primary_type: String, Description:String,Location_description:String, Arrest : Boolean, Domestic :Boolean,Beat: String,District: String,Ward: String, community: String,Fbicode: String, X_Co_ordinate: String,Y_Co_ordinate: String, Year: Int,Updated_on: String,lattitude : Double, longititude: Double,location : String ) extends Serializable with Ordered[CrimeRecord]{
   def compare(that:CrimeRecord) = id.compare(that.id)

 }

 def parse(row: String): CrimeRecord = {
   val fields = row.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)",-1)
   val id: Int = fields(0).toInt
   val Case_Number: String = fields(1)
   val Date : String = fields(2)
   val Block :String = fields(3) 
   val IUCR : String = fields(4)
   val Primary_type : String = fields(5)
   val Description : String = fields(6)
   val Location_description : String = fields(7)
   val Arrest : Boolean = fields(8).toBoolean
   val Domestic : Boolean = fields(9).toBoolean
   val Beat :String = fields(10)
   val District :String = fields(11)
   val Ward : String = fields(12)
   val community :String = fields(13)
   val Fbicode :String = fields(14)
   val X_Co_ordinate : String = fields(15)
   val Y_Co_ordinate : String = fields(16)
   val Year : Int = fields(17).toInt
   val Updated_on : String = fields(18)
   var lattitude : Double = Double.NaN
   var longititude : Double = Double.NaN
   if(fields(19) != "") { lattitude = fields(19).toDouble;}
   if(fields(20) != "") { longititude  = fields(20).toDouble;}
   val location : String = "("+lattitude+", "+longititude+")"
   // val location : String = fields(21)

   CrimeRecord(id,Case_Number,Date,Block,IUCR,Primary_type,Description,Location_description,Arrest,Domestic,Beat,District,Ward,community,Fbicode,X_Co_ordinate,Y_Co_ordinate,Year,Updated_on,lattitude,longititude,location)
  
 }
 
     override def main(arg: Array[String]): Unit = {
  

   var sparkConf = new SparkConf().setMaster("local").setAppName("CrimeAnalytics")
   var sc = new SparkContext(sparkConf)
       val spark = SparkSession
      .builder()
      .appName("test")
      .master("local[2]")
      .getOrCreate()
      

   val resource = getClass.getResourceAsStream("/Crime_dataset.csv")
    if (resource == null) sys.error("Please download the  dataset ")
     val RddRaw =  sc.parallelize(Source.fromInputStream(resource).getLines().toList)
    // val RddRaw = sc.textFile("file:///home/suket/case_studies/CrimeAnalytics/src/main/resources/Crime_dataset.csv") 
      val CrimeRdd = RddRaw.map(parse)
     import spark.implicits._
     val CrimeDS = CrimeRdd.toDS
     val CrimesInEachFbicode : Array[(String, Long)] = CrimeDS.groupBy($"Fbicode").count().collect.map {row => (row(0).asInstanceOf[String],row(1).asInstanceOf[Long])} 
   

     def windowSpec = Window.partitionBy("District", "Fbicode")                                  //defining a window frame for the aggregation

     val MostFrequentCrimesinEachDistrict : Array[(String, String)]=  CrimeDS.withColumn("count", count("Fbicode").over(windowSpec))     // counting repeatition of Fbicode for each group of District, Fbicode and assigning that Fbicode to new column called as count
                                                                      .orderBy($"count".desc)                                   // order dataframe with count in descending order
                                                                      .groupBy("District")                                           // group by District
                                                                      .agg(first("Fbicode").as("Fbicode"))                         //taking the first row of each key with count column as the highest
                                                                      .collect.map {row => (row(0).asInstanceOf[String],row(1).asInstanceOf[String])}                        
    
     
      // ******************************* Wrong Syntax ******************** 
      // val YearWiseCrimeUnderFbicode = CrimeDS.groupBy($"Year").groupBy($"Fbicode").count


    val Years = CrimeDS.groupBy($"Year").count.collect.map {row => row(0).asInstanceOf[Int]}.sorted
    var FbiCasesYearWise:Map[Int,Array[(String,Long)]] = Map()

    for ( count <- 0 to Years.length-1){
        FbiCasesYearWise+=( Years(count) -> CrimeDS.filter($"Year" === Years(count)).groupBy($"Fbicode").count.collect.map {row => (row(0).asInstanceOf[String],row(1).asInstanceOf[Long])} )

    }
    

     // FbiCasesYearWise(2015).sortWith(_._2 >_._2 )

    
    val NarcoticsCasesin2015 : Array[CrimeRecord] = CrimeDS.filter(($"Primary_type" === "NARCOTICS") && ($"Year" === 2015)).collect
   
     val AllNarcoticsCases = CrimeDS.filter($"Primary_type" === "NARCOTICS")
    
     val FbicodeWithMaxNarcoticsCases : Array[(String, Long)] =  AllNarcoticsCases.groupBy($"Fbicode").count.orderBy($"count".desc).limit(1).collect.map {row => (row(0).asInstanceOf[String],row(1).asInstanceOf[Long])} // (code,count)

    val TheftRelatedArrestsInEachDistrict : Array[(String, Long)] =  CrimeDS.filter(($"Primary_type" === "THEFT") && ($"Arrest" === true)).groupBy($"District").count.collect.map {row => (row(0).asInstanceOf[String],row(1).asInstanceOf[Long])}// (District, Arrest_count)
  
       println()
    println("Number of Crimes in each FBI code : ")
    println()
     for ( count <- 0 to CrimesInEachFbicode.length-1){

                         var tuple = CrimesInEachFbicode(count)
                         // println(tuple)
                         var Fbicode = tuple._1
                         var Crimecount = tuple._2
                         println(f"Fbicode = $Fbicode%4s    Number of Crimes = $Crimecount")
                        

                        }

       println()
        println("Most frequent crimes in District : ")
    println()
     for ( count <- 0 to MostFrequentCrimesinEachDistrict.length-1){

                         var tuple = MostFrequentCrimesinEachDistrict(count)
                         // println(tuple)
                         var  District= tuple._1
                         var Fbicode = tuple._2
                         println(f"District = $District%4s    Most frequent FBI Crime Code  = $Fbicode")
                        

                        }  
          println()                 
             println("Number of Crimes in each FBI code : ")
    println()
     for ( count <- 0 to TheftRelatedArrestsInEachDistrict.length-1){

                         var tuple = TheftRelatedArrestsInEachDistrict(count)
                         // println(tuple)
                         var District = tuple._1
                         var Arrestcount = tuple._2
                         println(f"District = $District%4s    Number of Arrests for Theft = $Arrestcount")
                        

                        }
  sc.stop()
    }
}