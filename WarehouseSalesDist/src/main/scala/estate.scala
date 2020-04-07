
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object estate {

  //Η getHaversineDistance παίρνει 2 συντεταγμένες και επιστρέφει την
  //χιλιομετρική τους απόσταση.Την υπολογίζει με τον τύπο του Haversine
  //δεν είναι ακριβής ο τύπος (λόγο του ελλειψοειδείς της γής ) αλλά κάνει
  //για την εργασία
  def getHaversineDistance( la1:Double, lo1:Double, la2:Double, lo2:Double ) : Double = {
    val R:Double = 6373.0

    val lat1: Double = la1.toRadians
    val lon1: Double = lo1.toRadians
    val lat2: Double = la2.toRadians
    val lon2: Double = lo2.toRadians

    val dlon: Double = lon2 - lon1
    val dlat: Double = lat2 - lat1

    val a:Double = Math.pow(Math.sin(dlat / 2), 2) + Math.cos(lat1) * Math.cos(lat2) * Math.pow(Math.sin(dlon / 2), 2)

    val c:Double  = 2 * Math.atan2( Math.sqrt(a), Math.sqrt(1 - a) )

    val distance: Double = R*c

    distance
  }

  def main(args: Array[String]) :Unit ={

    //Ορισμός της μεταβλητής για την απόσταση των 2 σημείων
    val DIST_IN_KM: Int = 5

    //Must Return Value : 278.54558935106695
    println( "Returned Value : " + getHaversineDistance( 52.2296756, 21.0122287, 52.406374, 16.9251681 ) );
    //Must Return Value : 202.08421362425355
    println( "Returned Value : " + getHaversineDistance( 39.441424, 20.272430, 39.274852, 22.612347 ) );

    //αφαιρεση καταγραφής-εκτύπωσης πληροφοριών
    //remove printing log info
    import org.apache.log4j._
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    //Δημιουργία ρυθμίσεις για το spark
    val ss = SparkSession.builder().master("local").appName("estateApp").getOrCreate()

    import ss.implicits._
    //Το όνομα του αρχείου που θα διαβάσει
    val inputFile = "estate.csv"
    println("reading from input file " + inputFile)

    //Διαβάσμα από το αρχείο και ορισμός κεφαλίδων
    val basicDF = ss.read.option("header", true).csv(inputFile)

    //Εκτύπωση αρχικού σχήματος και δείγμα από τα δεδομένα
    basicDF.printSchema()
    basicDF.show()

    //Ορισμός χρήσιμων για την βάση User-Defined Fuction(UDFs)
    val udf_toDouble = udf[Double, String](_.toDouble)
    val udf_toInt = udf[Int, String](_.toInt)

    //Ορισμός UDF για την συνάρτηση getHaversineDistance
    //val udf_getHaversineDistance = udf[Double, Double, Double]
    val udf_getHaversineDistance = udf[Double, Double, Double, Double, Double]( getHaversineDistance )

    //Μετατροπή των στηλών σε σωστό τύπο δεδομένων
    val modifieDF = basicDF
      .withColumn("zip", udf_toInt($"zip"))
      .withColumn("beds", udf_toInt($"beds"))
      .withColumn("baths", udf_toInt($"baths"))
      .withColumn("sq__ft", udf_toInt($"sq__ft"))
      .withColumn("price", udf_toInt($"price"))
      .withColumn("latitude", udf_toDouble($"latitude"))
      .withColumn("longitude", udf_toDouble($"longitude"))

    //Εκτύπωση του μετασχηματισμένου σχήματος και δειγμά από αυτό
    modifieDF.printSchema()
    modifieDF.show()

    //Δημιουργία ή Αντικατάσταση του  πίνακα σε προσωρινή προβολή με το όνομα estate
    modifieDF.createOrReplaceTempView("estate")

    //Δημιουργίας σύνδεσης του πίνακα modifieDF με τον εαυτό του και μετονομασία των πεδίων
    val modifieDF_join = modifieDF.crossJoin(modifieDF).toDF(
      "street", "city", "zip", "state", "beds", "baths", "sq__ft", "type", "sale_date", "price", "latitude", "longitude",
      "street_2", "city_2", "zip_2", "state_2", "beds_2", "baths_2", "sq__ft_2", "type_2", "sale_date_2", "price_2", "latitude_2", "longitude_2"
    ).where("street <> street_2")
    
    //Εκτύπωση του σχήματος με την σύνδεση και δειγμά από αυτό
    modifieDF_join.show()

    //Προσθήκη ενός πεδίου με το όνομα Distance και ενημέρωση του πεδίου με τη
    //συνάρτηση getHaversineDistance και είσοδο των 2 συντεταγμένων από την κάθε εγγραφή
    val modifieDF_with_dist = modifieDF_join.withColumn(
      "Distance",
      udf_getHaversineDistance(
        $"latitude",
        $"longitude",
        $"latitude_2",
        $"longitude_2"
      )
    )

    //Εκτύπωση του μετασχηματισμένου σχήματος με την απόσταση και δειγμά από αυτό
    modifieDF_with_dist.show()

    //Εκτύπωση δείγματος του πίνακα με το πεδίο Distance οπου η απόσταση των 2 σημείων είναι
    //μικρότερη της μεταβλητής DIST_IN_KM
    modifieDF_with_dist.filter($"Distance" < DIST_IN_KM).show()

    println( "To πλήθος των αρχικών εγγραφών " + modifieDF.count() )
    println( "To πλήθος των εγγραφών της συνδεσης με join " + modifieDF_join.count())
    println( "To πλήθος των εγγραφών < "+ DIST_IN_KM+ " χλμ " + modifieDF_with_dist.filter($"Distance" < DIST_IN_KM).count())

    ss.close()
  }
}
