/**
 * Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"). You may not
 * use this file except in compliance with the License. A copy of the License
 * is located at
 *
 *     http://aws.amazon.com/apache2.0/
 *
 * or in the "license" file accompanying this file. This file is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 *
 */

package main.scala.com.amazon.deequ.repository.prometheus

import java.net.{HttpURLConnection, URL, URLEncoder}

import com.amazon.deequ.analyzers.runners.AnalyzerContext
import com.amazon.deequ.metrics.DoubleMetric
import com.amazon.deequ.repository.{AnalysisResult, AnalysisResultSerde, MetricsRepository, MetricsRepositoryMultipleResultsLoader, ResultKey}
import org.apache.xerces.impl.XMLEntityManager.Entity
import org.slf4j.LoggerFactory

class PrometheusMetricsRepository( url: String) extends MetricsRepository {

  lazy val logger = LoggerFactory.getLogger(this.getClass.getName)
  lazy val pg = PushGateway(url, "deequ-spark-job")

  override def save(resultKey: ResultKey, analyzerContext: AnalyzerContext): Unit = {
    val successfulMetrics = analyzerContext.metricMap
      .filter { case (_, metric) => metric.value.isSuccess }

    val res = AnalyzerContext.successMetricsAsJson(analyzerContext)
    logger.debug(res)
    logger.debug(successfulMetrics.mkString("|"))

    successfulMetrics.foreach(m => {
      val metric = m._2
      metric match {
        case DoubleMetric(entity, name, instance, value) => {
          val keys = resultKey.tags.values.mkString("_")
          val key = s"dq_${keys.toLowerCase}_${instance
            .replace("*","table")
            .replace(",","_")
            .replace(" ", "_")
            .toLowerCase}_${name.toLowerCase}"
          logger.debug(s"${key} ${value.get.toString}")
          pg.post(s"${key} ${value.get.toString}\n", null, "tool", "deequ")
        }
      }

    })





  }

  /**
   * Get a AnalyzerContext saved using exactly the same resultKey if present
   */
  override def loadByKey(resultKey: ResultKey): Option[AnalyzerContext] = ???

  /** Get a builder class to construct a loading query to get AnalysisResults */
  override def load(): MetricsRepositoryMultipleResultsLoader = ???
}

/**
 * serverIPnPort: String with prometheus pushgateway hostIP:Port,
 * metricsJob: job name
 */
case class PushGateway(url: String, metricsJob: String) {

  lazy val logger = LoggerFactory.getLogger(this.getClass.getName)

  var urlJob = s"DefaultJob"
  try {
    urlJob = URLEncoder.encode(metricsJob, s"UTF-8")
  } catch {
    case uee: java.io.UnsupportedEncodingException =>
      logger.error(s"metricsJob '$metricsJob' cannot be url encoded")
  }
  val urlBase = url + s"/metrics/job/" + urlJob

  val requestMethod = s"POST"
  val connectTimeout = 5000 // milliseconds
  val readTimeout = 5000 // milliseconds


  /**
   * name: name String, validChars: String with valid characters,
   * replace all other characters with '_",
   * remove leading and trailing white spaces,
   * if first symbol is digit, add leading '_".
   */
  def validateName(name: String, validChars: String): String = {

    if (name == null) return null

    val trimmedStr = name.replaceAll(validChars, s" ").trim
    var resultStr = trimmedStr.replaceAll(s"[ ]", s"_")
    if (resultStr.charAt(0).isDigit) resultStr = s"_" + resultStr
    resultStr

  }


  /**
   * name: String with label name,
   * replace all not valid characters with '_",
   * remove leading and trailing white spaces,
   * if first symbol is digit, add leading '_".
   */
  def validateLabel(name: String): String = {

    validateName(name, s"[^a-zA-Z0-9_]")

  }


  /**
   * name: String with metric name,
   * replace all not valid characters with '_",
   * remove leading and trailing white spaces,
   * if first symbol is digit, add leading '_".
   */
  def validateMetric(name: String): String = {

    validateName(name, s"[^a-zA-Z0-9_:]")

  }


  /**
   * metrics: String with metric name-value pairs each ending with eol,
   * metricsType: metrics type (task or stage),
   * labelName: metrics label name,
   * labelValue: metrics label value
   */
  def post(metrics: String, metricsType: String, labelName: String, labelValue: String): Unit = {

    var urlType = s"NoType"
    try {
      if ((metricsType != null) && (metricsType != ""))
        urlType = URLEncoder.encode(metricsType, s"UTF-8")
    } catch {
      case uee: java.io.UnsupportedEncodingException =>
        logger.warn(s"metricsType '$metricsType' cannot be url encoded, use default")
    }

    var urlLabelName = s"NoLabelName"
    if ((urlLabelName != null) && (urlLabelName != ""))
      urlLabelName = validateLabel(labelName)

    var urlLabelValue = s"NoLabelValue"
    try {
      if ((labelValue != null) && (labelValue != ""))
        urlLabelValue = URLEncoder.encode(labelValue, s"UTF-8")
    } catch {
      case uee: java.io.UnsupportedEncodingException =>
        logger.warn(s"labelValue '$labelValue' cannot be url encoded, use default")
    }
    val urlFull = urlBase + s"/" + urlLabelName + s"/" + urlLabelValue

    try {
      val connection = (new URL(urlFull)).openConnection.asInstanceOf[HttpURLConnection]
      connection.setConnectTimeout(connectTimeout)
      connection.setReadTimeout(readTimeout)
      connection.setRequestMethod(requestMethod)
      connection.setRequestProperty("Content-Type","text/plain; version=0.0.4")
      connection.setDoOutput(true)

      val outputStream = connection.getOutputStream
      if (outputStream != null) {
        outputStream.write(metrics.getBytes("UTF-8"))
        outputStream.flush();
        outputStream.close();
      }

      val responseCode = connection.getResponseCode()
      val responseMessage = connection.getResponseMessage()
      connection.disconnect();
      if (responseCode != 202) // 202 Accepted, 400 Bad Request
        logger.error(s"Data sent error, url: '$urlFull', response: $responseCode '$responseMessage'")
    } catch {
      case ioe: java.io.IOException =>
        println("java.io.IOException")
        logger.error(s"Data sent error, url: '$urlFull', " + ioe.getMessage())
      case ste: java.net.SocketTimeoutException =>
        println("java.net.SocketTimeoutException")
        logger.error(s"Data sent error, url: '$urlFull', " + ste.getMessage())
    }

  }
}
