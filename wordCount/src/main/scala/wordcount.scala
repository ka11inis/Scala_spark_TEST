
/* WordCount.scala */

import org.apache.spark.{SparkConf, SparkContext}
import scala.io.Source
object wordcount {
  def main(args: Array[String]): Unit = {
    //αφαιρεση καταγραφής-εκτύπωσης πληροφοριών
    //remove printing log info
    import org.apache.log4j._
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    //Διαβάζει τις stop words από το αρχείο StopWords.txt
    //τις μετατρέπει σε κείμενο και μετά σε πίνακα με διαχωριστικό το ,
    val stopWords = Source.fromFile("StopWords.txt")
      .mkString
      .split(",")


    println("***************************************************")
    println("***************************************************")

    println("Hi, this is the WordCount application for Spark.")

    // Δημιουργία ρυθμίσεις για το spark
    val sparkConf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("WordCount")

    // Δημιουργία context για το spark
    val sc = new SparkContext(sparkConf)  // create spark context

    //λάμβάνουμε τον τρέχοντα κατάλογο που τρέχει το πρόγραμμα
    val currentDir = System.getProperty("user.dir")  // get the current directory
    //Αποθήκευση της διαδρομής του shakespeare.txt στη μεταβλητή
    val inputFile = "file://" + currentDir + "/shakespeare.txt"
    //val outputDir = "file://" + currentDir + "/output"

    println("reading from input file: " + inputFile)

    //Διάβασμα του αρχειόυ shakespeare.txt από το τρέχοντα φακέλου
    val txtFile = sc.textFile( inputFile, 4)

    //Αποθήκευση των αποτελεσμάτων στη μεταβλητή
    val result = txtFile.
      flatMap(line => line.replaceAll("[^a-zA-Z0-9]"," ") //Διαγραφή όλων των σημείων στίξης
        .toLowerCase() //Μετατροπή όλων των χαρακτήρων σε πεζούς
        .split(" ") //Διαχωρισμός του κειμένου όπου έχει κενό
        // Φίλτρο για την αφαίρεση των stopword
        .filter(word => !stopWords.contains(word) && word !="" )  )
      // Χαρτογράφηση των λέξεων και σε ζευγάρι (κλειδιου , τιμής) (word, 1)
      .map(word => (word,1)) // map each word into a (word,1) pair
      // Εύρεση λέξεων με το ίδιο κλειδί και πρόσθεση της τιμής τους
      .reduceByKey(_+_) // reduce by key (sum the values of the same key)
      //Επιστρέφη ένα πίνακα που περιέχει όλα τα στοιχεία του RDD
      //Για να γράψουμε το RDD  σε αρχείο πρέπει να μην βάλουμε την collect
      .collect() // collect results back to the driver
      //Ταξινομεί τα στοιχεία σε αύξουσα διάταξη ως προς το πληθος εμφανίσεων
      .sortBy(w => w._2)
      //Εκτύπωση όλων των στοιχείων
      .foreach(println) // print results
      // Μπορούμε αν θέλουμε να αποθηκεύσουμε τα αποτελέσματα
      // και σε αρχείο είτε σε τοπικό φάκελο ή σε HDFS saveAsTextFile(outputDir)
      //.saveAsTextFile("Output")

    //Έλεγχος του πλήθους για το αν δουλέυουν τα φίλτρα
    //print(result.length)

    //Σταματάμε το spark
    sc.stop()

    println("***************************************************")
    println("***************************************************")

  }
}
