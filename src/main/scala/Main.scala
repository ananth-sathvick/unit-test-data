import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType, DateType,DoubleType};
//Deeque Metrics
import com.amazon.deequ.VerificationSuite
import com.amazon.deequ.checks.{Check, CheckLevel, CheckStatus}
import com.amazon.deequ.constraints.ConstraintStatus
import com.amazon.deequ.VerificationResult.checkResultsAsDataFrame


object Main extends App {

  //RDD 
  val conf = new SparkConf().setAppName("hello-world").setMaster("local");
  var sc = new SparkContext(conf);
  sc.setLogLevel("Off");
  val lines = sc.textFile("src/main/resources/netflix_titles.csv")
  val lineLengths = lines.count();
  

  //Spark SQL
  val spark = SparkSession
  .builder()
  .appName("hello-world")
  .getOrCreate()

  // val df2 = spark.read.options(Map("inferSchema"->"true","delimiter"->",","header"->"true","nullable","false"))
  // .csv("src/main/resources/netflix_titles.csv")

  // df2.printSchema();

  val df2 = spark.read.option("sep", ",")
    .option("inferSchema", "true")
    .option("header", "true")
    .option("nullValues","")
    // .option("mode", "DROPMALFORMED")  // Drop records not matching the csv , 
    .option("dateFormat","MMMM dd, yyyy")
    .csv("src/main/resources/net_dup.csv")
  df2.printSchema();
  // df2.show();

  // val linesSQL = spark.read.options(Map("inferSchema"->"true","delimiter"->","),"header",true)).csv("src/main/resources/netflix_titles.csv")

  //Create schema 
  val simpleSchema = StructType(Array(
    StructField("show_id",IntegerType,false),
    StructField("type",StringType,true),
    StructField("title",StringType,true),
    StructField("director", StringType, true),
    StructField("cast", StringType, true),
    StructField("country", StringType, true),
    StructField("date_added", DateType, true),
    StructField("release_year", IntegerType, true),
    StructField("rating", DoubleType, true),
    StructField("duration", StringType, true),
    StructField("listed_in", StringType, true),
    StructField("description", StringType, true)
  ));

  val df3 = spark.read.option("sep", ",")
    // .option("inferSchema", "true")
    .option("header", "true")
    .option("nullValues","")
    .option("mode", "DROPMALFORMED")  // Drop records not matching the csv , 
    /*
    PERMISSIVE (default): nulls are inserted for fields that could not be parsed correctly
    DROPMALFORMED: drops lines that contain fields that could not be parsed
    FAILFAST: aborts the reading if any malformed data is found
    */
    .option("dateFormat","MMMM d, yyyy") // Change date format 
    .schema(simpleSchema)
    .csv("src/main/resources/net_dup.csv")
  df3.printSchema();
  df3.show();

  
  

  sc.stop();
}