package no.fdk.fdk_public_service_harvester.adapter

import no.fdk.fdk_public_service_harvester.harvester.HarvestException
import no.fdk.fdk_public_service_harvester.model.HarvestDataSource
import org.springframework.http.HttpStatus
import org.springframework.stereotype.Service
import java.io.BufferedReader
import java.net.HttpURLConnection
import java.net.URI

private const val TEN_MINUTES = 600000

@Service
class ServicesAdapter {

    fun fetchServices(source: HarvestDataSource): String {
        with(URI(source.url).toURL().openConnection() as HttpURLConnection) {
            try {
                setRequestProperty("Accept", source.acceptHeaderValue)
                connectTimeout = TEN_MINUTES
                readTimeout = TEN_MINUTES

                return if (responseCode != HttpStatus.OK.value()) {
                    throw HarvestException("${source.url} responded with ${responseCode}, harvest will be aborted")
                } else {
                    inputStream.bufferedReader()
                        .use(BufferedReader::readText)
                }
            } finally {
                disconnect()
            }

        }
    }
}
