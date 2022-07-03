-- Databricks notebook source
-- MAGIC 
-- MAGIC %scala
-- MAGIC val tags = com.databricks.logging.AttributionContext.current.tags
-- MAGIC 
-- MAGIC //*******************************************
-- MAGIC // GET VERSION OF APACHE SPARK
-- MAGIC //*******************************************
-- MAGIC 
-- MAGIC // Get the version of spark
-- MAGIC val Array(sparkMajorVersion, sparkMinorVersion, _) = spark.version.split("""\.""")
-- MAGIC 
-- MAGIC // Set the major and minor versions
-- MAGIC spark.conf.set("com.databricks.training.spark.major-version", sparkMajorVersion)
-- MAGIC spark.conf.set("com.databricks.training.spark.minor-version", sparkMinorVersion)
-- MAGIC 
-- MAGIC //*******************************************
-- MAGIC // GET USERNAME AND USERHOME
-- MAGIC //*******************************************
-- MAGIC 
-- MAGIC // Get the user's name
-- MAGIC val username = tags.getOrElse(com.databricks.logging.BaseTagDefinitions.TAG_USER, java.util.UUID.randomUUID.toString.replace("-", ""))
-- MAGIC val userhome = s"dbfs:/user/$username"
-- MAGIC 
-- MAGIC // Set the user's name and home directory
-- MAGIC spark.conf.set("com.databricks.training.username", username)
-- MAGIC spark.conf.set("com.databricks.training.userhome", userhome)
-- MAGIC 
-- MAGIC //*******************************************
-- MAGIC // MOUNT BUCKET IF NOT ALREADY MOUNTED
-- MAGIC //*******************************************
-- MAGIC 
-- MAGIC val mountDir = "/mnt/davis"
-- MAGIC val source = "davis-dsv1071/data"
-- MAGIC 
-- MAGIC if (!dbutils.fs.mounts().map(_.mountPoint).contains(mountDir)) {
-- MAGIC   println(s"Mounting $source\n to $mountDir")
-- MAGIC   val accessKey = "AKIAJDDJNXBZ3FJHWLDQ"
-- MAGIC   val secretKey = "wD26wPuGshDAIObuOFKPBsQ8n5LPd0aJYWyS2H+%2F"
-- MAGIC   val sourceStr = s"s3a://$accessKey:$secretKey@$source"
-- MAGIC   
-- MAGIC   dbutils.fs.mount(sourceStr, mountDir)
-- MAGIC }
-- MAGIC 
-- MAGIC displayHTML("")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC username = spark.conf.get("com.databricks.training.username")
-- MAGIC userhome = spark.conf.get("com.databricks.training.userhome")
-- MAGIC 
-- MAGIC displayHTML("Data mounted to /mnt/davis ...")
