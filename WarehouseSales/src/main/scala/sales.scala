import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DecimalType

object sales {
  def main(args: Array[String]): Unit ={

    //αφαιρεση καταγραφής-εκτύπωσης πληροφοριών
    //remove printing log info
    import org.apache.log4j._
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    //Δημιουργία ρυθμίσεις για το spark
    val ss = SparkSession.builder().master("local").appName("salesApp").getOrCreate()

    import ss.implicits._
    //Το όνομα του αρχείου που θα διαβάσει
    val inputFile = "sales.csv"
    println("reading from input file " + inputFile)

    //Διαβάσμα από το αρχείο και ορισμός κεφαλίδων
    val basicDF = ss.read.option("header", true).csv(inputFile)
    //Εκτύπωση αρχικού σχήματος και δείγμα από τα δεδομένα
    basicDF.printSchema()
    basicDF.show()

    //Ορισμός χρήσιμων για την βάση User-Defined Fuction(UDFs)
    val udf_toDouble = udf[Double, String](_.toDouble)
    val udf_toInt = udf[Int, String](_.toInt)

    //Μετατροπή των στηλών σε σωστό τύπο δεδομένων
    val modifieDF = basicDF
      .withColumn("Order ID", udf_toInt($"Order ID"))
      .withColumn("Units Sold", udf_toInt($"Units Sold"))
      .withColumn("Unit Price", udf_toDouble($"Unit Price"))
      .withColumn("Unit Cost", udf_toDouble($"Unit Cost"))
      .withColumn("Total Revenue", udf_toDouble($"Total Revenue"))
      .withColumn("Total Cost", udf_toDouble($"Total Cost"))
      .withColumn("Total Profit", udf_toDouble($"Total Profit").cast(DecimalType(15,4)))

    //Εκτύπωση του μετασχηματισμένου σχήματος και δειγμά από αυτό
    modifieDF.printSchema()
    modifieDF.show()

    //Δημιουργία ή Αντικατάσταση του  πίνακα σε προσωρινή προβολή με το όνομα sales
    modifieDF.createOrReplaceTempView("sales")

    //Oi SQL εντολές για τα raw sql query για τα ανάλογα υποερωτήματα (ΑΒΓΔ)
    val SQL_A_SalesFromEurope =
      """SELECT * From sales WHERE Region = 'Europe'"""
    val SQL_B_TotalProfitByCountry =
      """SELECT Region,sum(`Total Profit`) From sales Group By Region"""
    val SQL_C_CountAvgByCountry =
      """SELECT Country,count(`Order ID`) as Count_Sales, avg(`Total Profit`) as Average_Profit From sales Group By Country"""
    val SQL_D_CountOfHSumProfitByCountry =
      """SELECT Country,Count(`ORDER ID`) as CountOfSales,sum(`Total Profit`) FROM (SELECT `Order ID`,Country,`Total Profit` FROM sales WHERE `Order Priority`='H') GROUP BY Country ORDER BY Country"""

    //Εμφανίζει όλες τις πωλήσεις που πραγματοποιήθηκαν στην περιοχή της Ευρώπης
    println("Όλες οι πωλήσεις που έγιναν στην Ευρώπη ")
    //1ος τρόπος από το spark df
    modifieDF.select("*").where($"Region" === "Europe").show()
    //2ος τρόπος με raw sql query
    modifieDF.sqlContext.sql(SQL_A_SalesFromEurope).show()

    //Εμφανίζει το συνολικό κέρδος απο τις πωλήσεις ανά περιοχή (Region)
    println("Συνολικό κέρδος από τις πωλήσεις ανά περιοχή")
    //1ος τρόπος από το spark df
    modifieDF.select("*").groupBy($"Region").sum("Total Profit").show()
    //2ος τρόπος με raw sql query
    modifieDF.sqlContext.sql(SQL_B_TotalProfitByCountry).show()
    //Εμφανίζει το πλήθος των πωλήσεων και το μέσο κέρδος ανά χώρα(Country)
    println("Συνολικό πλήθος πωλήσεων και μέσο κέρδος ανά χώρα")
    modifieDF
      .groupBy($"Country")
      .agg(
        count($"Total Profit").as("Count_Sales"),
        avg($"Total Profit").as("Average_Profit")
      ).show()
    //2ος τρόπος με raw sql query
    modifieDF.sqlContext.sql(SQL_C_CountAvgByCountry).show()

    //Εμφανίζει το πλήθος των παραγγελιών υψηλής προτεραιότητας (Η) και το
    //συνολικό κέρδος από αυτές, ανά χώρα και το αποτέλεσμα ταξινομημένο
    //σε αυξουσα διάταξη ως προς το όνομα της χώρας
    println("Συνολικό πλήθος παραγγελιών υψηλής προτεραιότητας (Η) και συνολικό κέρδος " +
      "ανά χώρα και το αποτέλεσμα ταξινομημένο σε αυξουσα διάταξη ως προς το όνομα της χώρας")
    //1ος τρόπος από το spark df
    modifieDF
      .select("*")
      .filter($"Order Priority" === "H")
      .groupBy("Country")
      .agg(
        count("Order ID").as("CountOfSales"),
        sum("Total Profit")
      )
      .sort("Country")
      .show()
    //2ος τρόπος με raw sql query
    modifieDF.sqlContext.sql(SQL_D_CountOfHSumProfitByCountry).show()

    ss.close()
  }
}
