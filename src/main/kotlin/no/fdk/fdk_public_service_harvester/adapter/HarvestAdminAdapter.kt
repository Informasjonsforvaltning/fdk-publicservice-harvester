package no.fdk.fdk_public_service_harvester.adapter

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import no.fdk.fdk_public_service_harvester.configuration.ApplicationProperties
import no.fdk.fdk_public_service_harvester.model.HarvestDataSource
import org.slf4j.LoggerFactory
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.stereotype.Service
import java.io.BufferedReader
import java.net.HttpURLConnection
import java.net.URL

private val logger = LoggerFactory.getLogger(HarvestAdminAdapter::class.java)

@Service
class HarvestAdminAdapter(private val applicationProperties: ApplicationProperties) {

    fun urlWithParameters(params: Map<String, String>?): URL =
        if (params != null && params.isNotEmpty()) {
            URL("${applicationProperties.harvestAdminRootUrl}/datasources?${
                params.map { "${it.key}=${it.value}" }.joinToString("&")
            }")
        } else URL("${applicationProperties.harvestAdminRootUrl}/datasources")

    fun getDataSources(queryParams: Map<String, String>?): List<HarvestDataSource> {
        val url = urlWithParameters(queryParams)
        with(url.openConnection() as HttpURLConnection) {
            try {
                setRequestProperty("Accept", MediaType.APPLICATION_JSON.toString())
                setRequestProperty("Content-type", MediaType.APPLICATION_JSON.toString())

                if (HttpStatus.valueOf(responseCode).is2xxSuccessful) {
                    val body = inputStream.bufferedReader().use(BufferedReader::readText)
                    return jacksonObjectMapper().readValue(body)
                } else {
                    logger.error("Fetch of harvest urls from $url failed, status: $responseCode")
                }
            } catch (ex: Exception) {
                logger.error("Error fetching harvest urls from $url", ex)
            } finally {
                disconnect()
            }
            return emptyList()
        }
    }

}
