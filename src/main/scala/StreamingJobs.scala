// Basic Spark imports
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ConstantInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

// Spark Streaming + Kafka imports
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.kafka.common.serialization.StringDeserializer

// Spark SQL Cassandra imports
import com.datastax.spark.connector.streaming._
import com.datastax.spark.connector._
import com.datastax.spark.connector.SomeColumns

// JSON parsing
import spray.json._
import java.util.{UUID, Date}

// ML Lib
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.{LabeledPoint, StreamingLinearRegressionWithSGD}

// Logger
import org.apache.log4j.Logger
import org.apache.log4j.Level

case class EEGData(pk: UUID, userid: String, calibrationid: String, eeg: List[String], date: Date, label: String)

object ClassificationJob {
  val numFeatures = 6
  val model = new StreamingLinearRegressionWithSGD()
    .setInitialWeights(Vectors.zeros(numFeatures))

  def main(args: Array[String]): Unit = {

    val localExecution = sys.env("LOCAL_EXECUTION")
    if (localExecution != null)
      System.setProperty("hadoop.home.dir", "C:\\winutils\\")

    val sparkMaster = sys.env.get("SPARK_MASTER")
    val sparkMasterPort = sys.env.get("SPARK_MASTER_PORT")

    val kafkaBroker = sys.env.get("KAFKA_BROKER")
    val kafkaBrokerPort = sys.env.get("KAFKA_BROKER_PORT")
    val kafkaCalibrationTopic = sys.env.get("KAFKA_CALIBRATION_TOPIC")

    val dbHost = sys.env.get("DB_HOST")
    val dbPort = sys.env.get("DB_PORT")
    val dbUsername = sys.env.get("DB_USERNAME")
    val dbPassword = sys.env.get("DB_PASSWORD")
    val dbKeyspaceName = sys.env.get("KEYSPACE_NAME")

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
  
    val sparkConf = new SparkConf()

    if (localExecution != null) {
      sparkConf.setMaster("local[2]")
    } else {
      sparkConf.setMaster("spark://" + sparkMaster.getOrElse("spark-master") + ":" + sparkMasterPort.getOrElse("7077"))
    }

    sparkConf.setAppName("EEGJob")
      .set("spark.driver.allowMultipleContexts", "true")
      .set("spark.cassandra.connection.host", dbHost.getOrElse("localhost"))
      .set("spark.cassandra.connection.port", dbPort.getOrElse("9042"))
      .set("spark.cassandra.auth.username", dbUsername.getOrElse("match_making_user"))
      .set("spark.cassandra.auth.password", dbPassword.getOrElse("match_making_pw"))

    val streamingContext = new StreamingContext(sparkConf, Seconds(20))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> (kafkaBroker.getOrElse("localhost") + ":" + kafkaBrokerPort.getOrElse("9092")),
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> (kafkaCalibrationTopic.getOrElse("eeg-calibration") + "_stream"),
      "auto.offset.reset" -> "latest", // "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    saveEEGClassificationData(streamingContext, kafkaCalibrationTopic, kafkaParams, dbKeyspaceName)
    generateMLModels(streamingContext, dbKeyspaceName)
    classifyEEGData(streamingContext, kafkaCalibrationTopic, kafkaParams, dbKeyspaceName)

    streamingContext.start()
    streamingContext.awaitTermination()
  }

  def classifyEEGData(streamingContext: StreamingContext, kafkaCalibrationTopic: Option[String], kafkaParams : Map[String, Object], dbKeyspaceName: Option[String]): Unit = {


  }

  def saveEEGClassificationData(streamingContext: StreamingContext, kafkaCalibrationTopic: Option[String], kafkaParams : Map[String, Object], dbKeyspaceName: Option[String]): Unit = {

    val topics = Array(kafkaCalibrationTopic.getOrElse("eeg-calibration"))
    val stream = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    val eegRows = stream.map { record =>
      val json = record.value().parseJson

      val eeg = json.asJsObject.getFields("DataPacketValue").head.toString().replaceAll("[\\[\\]]","").replaceAll("\\s", "").split(",").toList

      val eegData = EEGData(
        UUID.randomUUID(),
        json.asJsObject.getFields("userId").head.toString(),
        json.asJsObject.getFields("calibrationId").head.toString(),
        eeg,
        new Date(),
        json.asJsObject.getFields("Label").head.toString()
      )

      eegData
    }
    eegRows.print()
    eegRows.saveToCassandra(dbKeyspaceName.getOrElse("match_making").toString, "user_calibrations_eeg_data", SomeColumns("pk", "userid", "calibrationid", "eeg", "date", "label"))

  }

  def generateMLModels(streamingContext: StreamingContext, dbKeyspaceName: Option[String]): Unit = {

    val ks = dbKeyspaceName.getOrElse("match_making").toString

    val userCalibrationRDD = {
         streamingContext.cassandraTable(ks, "user_calibrations")
           .select("calibrationid")
           .where("modelsGenerated = ?", false)
           .keyBy( r => r.getString("calibrationid"))
    }

    val userCalibrationEEGDataRDD = {
      streamingContext.cassandraTable(ks, "user_calibrations_eeg_data")
           .select("eeg", "label", "calibrationid")
           .keyBy( r => r.getString("calibrationid") )
       }


    val joinedRDD = userCalibrationRDD.join(userCalibrationEEGDataRDD)
      .map{ case (key, value) => (key, value._2)}
      .groupByKey
      .map(row => {
        row._2.map(data => {
          val label = data.getString("label").toString
          var labelNum = 0
          if (label == "\"Medium\""){
            labelNum = 1
          } else if (label == "\"Shallow\"") {
            labelNum = 2
          } else if (label == "\"Deep\"") {
            labelNum = 3
          } else {
            labelNum = 4
          }
          val features = data.getList[String]("eeg").map(d => d.toDouble).toArray
          LabeledPoint(labelNum.toDouble, Vectors.dense(features))
        })
      })
      .flatMap(x => x)

    print(joinedRDD.count())
    if (joinedRDD.count() > 0) {
      val splits = joinedRDD.randomSplit(Array(0.7, 0.3))

      val trainingData = new ConstantInputDStream(streamingContext, splits(0))
      val testData = new ConstantInputDStream(streamingContext, splits(1))

      model.trainOn(trainingData)
      model.predictOnValues(testData.map(lp => (lp.label, lp.features))).print()
    }

  }

}


