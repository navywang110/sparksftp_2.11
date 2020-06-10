/*
 * Copyright 2015 springml
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.springml.spark.sftp

import java.io.File
import java.util.{Properties, UUID}

import com.jcraft.jsch.ChannelSftp
import com.jcraft.jsch.JSch
import com.jcraft.jsch.JSchException
import com.jcraft.jsch.SftpException
import com.springml.sftp.client.SFTPClient
import com.springml.spark.sftp.util.Utils.ImplicitDataFrameWriter
import org.apache.commons.io.FilenameUtils
import org.apache.hadoop.fs.Path
import org.apache.log4j.Logger
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, RelationProvider, SchemaRelationProvider}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

/**
 * Datasource to construct dataframe from a sftp url
 */
class DefaultSource extends RelationProvider with SchemaRelationProvider with CreatableRelationProvider  {
  @transient val logger = Logger.getLogger(classOf[DefaultSource])

  private val timeout = 60000 //超时数,一分钟
  /**
   * Copy the file from SFTP to local location and then create dataframe using local file
   */
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]):BaseRelation = {
    createRelation(sqlContext, parameters, null)
  }

  /**
   * Copy the file from SFTP to local location and then create dataframe using local file
   */
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String], schema: StructType) = {
    val appId = parameters.getOrElse("appId","application")
    val stepId = parameters.getOrElse("stepId","sftp")
    val newAppId = appId + "_" + stepId
    val username = parameters.get("username")
    val password = parameters.get("password")
    val pemFileLocation = parameters.get("pem")
    val pemPassphrase = parameters.get("pemPassphrase")
    val host = parameters.getOrElse("host", sys.error("SFTP Host has to be provided using 'host' option"))
    val port = parameters.get("port")
    val path = parameters.getOrElse("path", sys.error("'path' must be specified"))
    val fileType = parameters.getOrElse("fileType", sys.error("File type has to be provided using 'fileType' option"))
    val inferSchema = parameters.get("inferSchema")
    val header = parameters.getOrElse("header", "true")
    val delimiter = parameters.getOrElse("delimiter", ",")
    val quote = parameters.getOrElse("quote", "\"")
    val escape = parameters.getOrElse("escape", "\\")
    val multiLine = parameters.getOrElse("multiLine", "false")
    val createDF = parameters.getOrElse("createDF", "true")
    val copyLatest = parameters.getOrElse("copyLatest", "false")
    val tempFolder = parameters.getOrElse("tempLocation", System.getProperty("java.io.tmpdir"))
    val hdfsTemp = parameters.getOrElse("hdfsTempLocation", tempFolder)
    val cryptoKey = parameters.getOrElse("cryptoKey", null)
    val cryptoAlgorithm = parameters.getOrElse("cryptoAlgorithm", "AES")
    val rowTag = parameters.getOrElse(constants.xmlRowTag, null)

    val supportedFileTypes = List("csv", "json", "avro", "parquet", "txt", "xml","orc")
    if (!supportedFileTypes.contains(fileType)) {
      sys.error("fileType " + fileType + " not supported. Supported file types are " + supportedFileTypes)
    }

    val inferSchemaFlag = if (inferSchema != null && inferSchema.isDefined) {
      inferSchema.get
    } else {
      "false"
    }

    val sftpClient = getSFTPClient(username, password, pemFileLocation, pemPassphrase, host, port,
      cryptoKey, cryptoAlgorithm)
    //新建本地tmp层级目录,一个flow如果包含多个sftp source需要区分开
    var temDir = tempFolder + File.separator + newAppId
    val target = temDir+ File.separator + FilenameUtils.getName(path)

    var isDir : Boolean = false
    try{
      var channel:ChannelSftp = getChannel(getValue(username),getValue(password),host,getValue(port))
      channel.cd(path)//目录
      isDir = true
      if(!new File(target).exists()){
        new File(target).mkdirs() //拷贝sftp目录到本地系统的话,需要在tmp下提前新建该文件夹
      }
    }catch {
      case e : SftpException => {//文件
        if(!new File(temDir).exists()) new File(temDir).mkdirs() //单个文件直接拷贝到本地系统的tmp/appId_stepId目录下
      }
    }
    logger.info("transfer sftp file to local system, source {}, target {}", path, target)
    //拷贝sftp文件或目录到本地系统,返回本地系统的文件全路径或全目录路径
    val copiedFileLocation = copy(sftpClient, path, temDir, copyLatest.toBoolean)
    //上传本地系统文件或目录到hdfs,如果是具体文件直接使用该名称,若是目录则会拷贝到该目录下
    var hdfsTmpDir : String = hdfsTemp + File.separator + newAppId + path
    val fileLocation = copyToHdfs(sqlContext, copiedFileLocation, hdfsTmpDir, hdfsTemp + File.separator + newAppId)

    if (!createDF.toBoolean) {
      logger.info("Returning an empty dataframe after copying files...")
      createReturnRelation(sqlContext, schema)
    } else {
      DatasetRelation(fileLocation, fileType, inferSchemaFlag, header, delimiter, quote, escape, multiLine, rowTag, schema,
        sqlContext)
    }
  }

  /**
    * DF sink to SFTP
    * @param sqlContext
    * @param mode
    * @param parameters
    * @param data
    * @return
    */
  override def createRelation(
      sqlContext: SQLContext,
      mode: SaveMode,
      parameters: Map[String, String],
      data: DataFrame): BaseRelation = {

    val username = parameters.get("username")
    val password = parameters.get("password")
    val pemFileLocation = parameters.get("pem")
    val pemPassphrase = parameters.get("pemPassphrase")
    val host = parameters.getOrElse("host", sys.error("SFTP Host has to be provided using 'host' option"))
    val port = parameters.get("port")
    val path = parameters.getOrElse("path", sys.error("'path' must be specified"))
    val fileType = parameters.getOrElse("fileType", sys.error("File type has to be provided using 'fileType' option"))
    val header = parameters.getOrElse("header", "true")
    val copyLatest = parameters.getOrElse("copyLatest", "false")
    val tmpFolder = parameters.getOrElse("tempLocation", System.getProperty("java.io.tmpdir"))
    val hdfsTemp = parameters.getOrElse("hdfsTempLocation", tmpFolder)
    val cryptoKey = parameters.getOrElse("cryptoKey", null)
    val cryptoAlgorithm = parameters.getOrElse("cryptoAlgorithm", "AES")
    val delimiter = parameters.getOrElse("delimiter", ",")
    val quote = parameters.getOrElse("quote", "\"")
    val escape = parameters.getOrElse("escape", "\\")
    val multiLine = parameters.getOrElse("multiLine", "false")
    val codec = parameters.getOrElse("codec", null)
    val rowTag = parameters.getOrElse(constants.xmlRowTag, null)
    val rootTag = parameters.getOrElse(constants.xmlRootTag, null)

    val supportedFileTypes = List("csv", "json", "avro", "parquet", "txt", "xml","orc")
    if (!supportedFileTypes.contains(fileType)) {
      sys.error("fileType " + fileType + " not supported. Supported file types are " + supportedFileTypes)
    }

    val sftpClient = getSFTPClient(username, password, pemFileLocation, pemPassphrase, host, port,
      cryptoKey, cryptoAlgorithm)
    val tempFile = writeToTemp(sqlContext, data, hdfsTemp, tmpFolder, fileType, header, delimiter, quote, escape, multiLine, codec, rowTag, rootTag)

    upload(tempFile, path, sftpClient)
    return createReturnRelation(data)
  }
  private def copyToHdfs(sqlContext: SQLContext, fileLocation : String,
                         hdfsTemp : String, deletePath : String): String  = {
    val hadoopConf = sqlContext.sparkContext.hadoopConfiguration
    val hdfsPath = new Path(fileLocation)
    val fs = hdfsPath.getFileSystem(hadoopConf)
    if ("hdfs".equalsIgnoreCase(fs.getScheme)) {
      logger.info("copy local file to hdfs, source {} ,target {}", fileLocation, hdfsTemp)
      fs.copyFromLocalFile(new Path(fileLocation), new Path(hdfsTemp))
//      val filePath = hdfsTemp
      fs.deleteOnExit(new Path(deletePath))
      return hdfsTemp
    } else {
      return fileLocation
    }
  }

  private def copyFromHdfs(sqlContext: SQLContext, hdfsTemp : String,
                           fileLocation : String): String  = {
    val hadoopConf = sqlContext.sparkContext.hadoopConfiguration
    val hdfsPath = new Path(hdfsTemp)
    val fs = hdfsPath.getFileSystem(hadoopConf)
    if (!"file".equalsIgnoreCase(fs.getScheme)) {
      fs.copyToLocalFile(new Path(hdfsTemp), new Path(fileLocation))
      fs.deleteOnExit(new Path(hdfsTemp))
      return fileLocation
    } else {
      return hdfsTemp
    }
  }

  private def upload(source: String, target: String, sftpClient: SFTPClient) {
    logger.info("Copying " + source + " to " + target)
    sftpClient.copyToFTP(source, target)
  }

  private def getSFTPClient(
      username: Option[String],
      password: Option[String],
      pemFileLocation: Option[String],
      pemPassphrase: Option[String],
      host: String,
      port: Option[String],
      cryptoKey : String,
      cryptoAlgorithm : String) : SFTPClient = {

    val sftpPort = if (port != null && port.isDefined) {
      port.get.toInt
    } else {
      22
    }

    val cryptoEnabled = cryptoKey != null

    if (cryptoEnabled) {
      new SFTPClient(getValue(pemFileLocation), getValue(pemPassphrase), getValue(username),
        getValue(password),
          host, sftpPort, cryptoEnabled, cryptoKey, cryptoAlgorithm)
    } else {
      new SFTPClient(getValue(pemFileLocation), getValue(pemPassphrase), getValue(username),
        getValue(password), host, sftpPort)
    }
  }

  private def createReturnRelation(data: DataFrame): BaseRelation = {
    createReturnRelation(data.sqlContext, data.schema)
  }

  private def createReturnRelation(sqlContextVar: SQLContext, schemaVar: StructType): BaseRelation = {
    new BaseRelation {
      override def sqlContext: SQLContext = sqlContextVar
      override def schema: StructType = schemaVar
    }
  }

  private def copy(sftpClient: SFTPClient, source: String,
      tempFolder: String, latest: Boolean): String = {
    var copiedFilePath: String = null
    try {
      val target = tempFolder + File.separator + FilenameUtils.getName(source)
      copiedFilePath = target
      if (latest) {
        copiedFilePath = sftpClient.copyLatest(source, tempFolder)
      } else {
        logger.info("Copying " + source + " to " + target)
        copiedFilePath = sftpClient.copy(source, target)
      }

      copiedFilePath
    } finally {
      addShutdownHook(copiedFilePath)
    }
  }

  private def getValue(param: Option[String]): String = {
    if (param != null && param.isDefined) {
      param.get
    } else {
      null
    }
  }

  private def writeToTemp(sqlContext: SQLContext, df: DataFrame,
                          hdfsTemp: String, tempFolder: String, fileType: String, header: String,
                          delimiter: String, quote: String, escape: String, multiLine: String, codec: String, rowTag: String, rootTag: String) : String = {
    val randomSuffix = "spark_sftp_connection_temp_" + UUID.randomUUID
    val hdfsTempLocation = hdfsTemp + File.separator + randomSuffix
    val localTempLocation = tempFolder + File.separator + randomSuffix

    addShutdownHook(localTempLocation)

    fileType match {

      case "xml" =>  df.coalesce(1).write.format(constants.xmlClass)
                    .option(constants.xmlRowTag, rowTag)
                    .option(constants.xmlRootTag, rootTag).save(hdfsTempLocation)
      case "csv" => df.coalesce(1).
                    write.
                    option("header", header).
                    option("delimiter", delimiter).
                    option("quote", quote).
                    option("escape", escape).
                    option("multiLine", multiLine).
                    optionNoNull("codec", Option(codec)).
                    csv(hdfsTempLocation)
      case "txt" => df.coalesce(1).write.text(hdfsTempLocation)
      case "avro" => df.coalesce(1).write.format("com.databricks.spark.avro").save(hdfsTempLocation)
      case _ => df.coalesce(1).write.format(fileType).save(hdfsTempLocation)
    }

    copyFromHdfs(sqlContext, hdfsTempLocation, localTempLocation)
    copiedFile(localTempLocation)
  }

  private def addShutdownHook(tempLocation: String) {
    logger.debug("Adding hook for file " + tempLocation)
    val hook = new DeleteTempFileShutdownHook(tempLocation)
    Runtime.getRuntime.addShutdownHook(hook)
  }

  private def copiedFile(tempFileLocation: String) : String = {
    val baseTemp = new File(tempFileLocation)
    val files = baseTemp.listFiles().filter { x =>
      (!x.isDirectory()
        && !x.getName.contains("SUCCESS")
        && !x.isHidden()
        && !x.getName.contains(".crc")
        && !x.getName.contains("_committed_")
        && !x.getName.contains("_started_")
        )
    }
    files(0).getAbsolutePath
  }


  def getChannel(username: String, password: String, host: String, port: String): ChannelSftp = {
    try {
      val jsch = new JSch // 创建JSch对象
      // 根据用户名，主机ip，端口获取一个Session对象
      var session = jsch.getSession(username, host, Integer.valueOf(port))
      logger.info("Session created...")
      if (password != null) session.setPassword(password) // 设置密码
      val config = new Properties
      config.put("StrictHostKeyChecking", "no")
      session.setConfig(config) // 为Session对象设置properties

      session.setTimeout(timeout) // 设置timeout时间

      session.connect() // 通过Session建立链接

      logger.info("Session connected, Opening Channel...")
      var channel = session.openChannel("sftp") // 打开SFTP通道

      channel.connect // 建立SFTP通道的连接

      logger.info("Connected successfully to ip :{}, ftpUsername is :{}, return :{}", host, username, channel)
      channel.asInstanceOf[ChannelSftp]
    }catch {
      case e: JSchException => logger.error(e)
        null
    }
  }

}
