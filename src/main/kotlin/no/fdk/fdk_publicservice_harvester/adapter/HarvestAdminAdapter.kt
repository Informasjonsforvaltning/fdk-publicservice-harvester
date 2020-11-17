package no.fdk.fdk_publicservice_harvester.adapter

import no.fdk.fdk_publicservice_harvester.configuration.ApplicationProperties
import no.fdk.fdk_publicservice_harvester.model.HarvestDataSource
import org.springframework.stereotype.Service

@Service
class HarvestAdminAdapter(private val applicationProperties: ApplicationProperties) {

    fun getDataSources(queryParams: Map<String, String>?): List<HarvestDataSource> {
        return listOf(
            HarvestDataSource(
                url = applicationProperties.tmpHarvest,
                acceptHeaderValue = "text/turtle",
                dataType = "service",
                dataSourceType = "DCAT-AP-NO"
            )
        )
    }

}