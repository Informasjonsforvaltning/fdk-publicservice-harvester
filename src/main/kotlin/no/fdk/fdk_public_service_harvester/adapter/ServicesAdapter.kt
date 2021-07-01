package no.fdk.fdk_public_service_harvester.adapter

import no.fdk.fdk_public_service_harvester.harvester.HarvestException
import no.fdk.fdk_public_service_harvester.model.HarvestDataSource
import org.slf4j.LoggerFactory
import org.springframework.http.HttpStatus
import org.springframework.stereotype.Service
import java.io.BufferedReader
import java.net.HttpURLConnection
import java.net.URL

private val LOGGER = LoggerFactory.getLogger(ServicesAdapter::class.java)

@Service
class ServicesAdapter {

    fun fetchServices(source: HarvestDataSource): String? {
        with(URL(source.url).openConnection() as HttpURLConnection) {
            try {
                setRequestProperty("Accept", source.acceptHeaderValue)

                return if (responseCode != HttpStatus.OK.value()) {
                    LOGGER.error("${source.url} responded with ${responseCode}, harvest will be aborted", HarvestException(source.url ?: "undefined"))
                    null
                } else {
                    inputStream.bufferedReader()
                        .use(BufferedReader::readText)
                }

            } catch (ex: Exception) {
                LOGGER.error("Error when harvesting from ${source.url}", ex)
                return null
            } finally {
                disconnect()
            }

        }
    }
}
