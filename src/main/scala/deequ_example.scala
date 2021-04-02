import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
//Deequ analyzer 
import com.amazon.deequ.analyzers.runners.{AnalysisRunner, AnalyzerContext}
import com.amazon.deequ.analyzers.runners.AnalyzerContext.successMetricsAsDataFrame
import com.amazon.deequ.analyzers.{Compliance, Correlation, Size, Completeness, Mean, ApproxCountDistinct}
import com.amazon.deequ.VerificationSuite
import com.amazon.deequ.checks.{Check, CheckLevel, CheckStatus}
import com.amazon.deequ.constraints.ConstraintStatus
import com.amazon.deequ.VerificationResult.checkResultsAsDataFrame

object deequ_example extends App {

    val spark = SparkSession
  .builder()
  .appName("hello-world")
  .getOrCreate()


    //Amazon deeque

  //Verification 
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
    // .schema(simpleSchema)
    .csv("src/main/resources/net_dup.csv")


   val verificationResult = VerificationSuite()
      .onData(df3)
      .addCheck(
        Check(CheckLevel.Error, "integrity checks")
          // we expect 5 records
          .hasSize(_ >= 5)
          // 'id' should never be NULL
          .isComplete("show_id")
          // 'id' should not contain duplicates
          .isUnique("show_id")
          // 'productName' should never be NULL
          .isComplete("title")
          // 'numViews' should not contain negative values
          .isNonNegative("rating"))
      .run()

      val resultDataFrame = checkResultsAsDataFrame(spark, verificationResult);
      resultDataFrame.show();
  
}
