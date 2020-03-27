

import org.apache.spark.{SparkConf, SparkContext}

object SumOfPoints {

  def main(args: Array[String]) {

    //αφαιρεση καταγραφής-εκτύπωσης πληροφοριών
    //remove printing log info
    import org.apache.log4j._
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    println("***************************************************")
    println("***************************************************")

    // Δημιουργία ρυθμίσεις για το spark
    val sparkConf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("SumCount")

    // Δημιουργία context για το spark
    val sc = new SparkContext(sparkConf)  // create spark context

    //λάμβάνουμε τον τρέχοντα κατάλογο που τρέχει το πρόγραμμα
    val currentDir = System.getProperty("user.dir")  // get the current directory
    //Αποθήκευση της διαδρομής του points.txt στη μεταβλητή
    val inputFile = "file://" + currentDir + "/points.txt"
    //val outputDir = "file://" + currentDir + "/output"
    println("reading from input file: " + inputFile)

    //Διάβασμα του αρχειόυ points.txt από το τρέχοντα φακέλου
    val txtFile = sc.textFile( inputFile, 4)

    //Αποθήκευση των αποτελεσμάτων στη μεταβλητή
    val result = txtFile
      // Χαρτογράφηση των γραμμών, διαχωρισμός των αριθμών και σβήσιμο των άδειων στοιχείων
      .map( line => line.split(" ").filterNot(_.isEmpty) )
      // Χαρτογράφηση της γραμμής και προσθεση των 2 αριθμών της κάθε γραμμής
      .map(w => w(0).toInt + w(1).toInt )
      // Χαρτογράφηση των λέξεων και σε ζευγάρι (κλειδιου , τιμής) (word, 1)
      .map(word => (word, 1) ) // map each word into a (word,1) pair
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
