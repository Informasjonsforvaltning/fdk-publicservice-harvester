package no.fdk.fdk_public_service_harvester.harvester

import no.fdk.fdk_public_service_harvester.configuration.ApplicationProperties
import no.fdk.fdk_public_service_harvester.adapter.ServicesAdapter
import no.fdk.fdk_public_service_harvester.repository.PublicServicesRepository
import no.fdk.fdk_public_service_harvester.model.*
import no.fdk.fdk_public_service_harvester.rdf.*
import no.fdk.fdk_public_service_harvester.service.TurtleService
import org.apache.jena.rdf.model.Model
import org.apache.jena.rdf.model.ModelFactory
import org.apache.jena.riot.Lang
import org.apache.jena.sparql.vocabulary.FOAF
import org.apache.jena.vocabulary.DCAT
import org.apache.jena.vocabulary.DCTerms
import org.apache.jena.vocabulary.RDF
import org.slf4j.LoggerFactory
import org.springframework.data.repository.findByIdOrNull
import org.springframework.stereotype.Service
import java.util.*

private val LOGGER = LoggerFactory.getLogger(PublicServicesHarvester::class.java)

@Service
class PublicServicesHarvester(
    private val adapter: ServicesAdapter,
    private val metaRepository: PublicServicesRepository,
    private val turtleService: TurtleService,
    private val applicationProperties: ApplicationProperties
) {

    fun harvestServices(source: HarvestDataSource, harvestDate: Calendar) =
        if (source.url != null) {
            LOGGER.debug("Starting harvest of ${source.url}")
            val jenaWriterType = jenaTypeFromAcceptHeader(source.acceptHeaderValue)

            val harvested = when (jenaWriterType) {
                null -> null
                Lang.RDFNULL -> null
                else -> adapter.fetchServices(source)?.let { parseRDFResponse(it, jenaWriterType, source.url) }
            }

            when {
                jenaWriterType == null -> LOGGER.error(Exception("Not able to harvest from ${source.url}, no accept header supplied").stackTraceToString())
                jenaWriterType == Lang.RDFNULL -> LOGGER.error(Exception("Not able to harvest from ${source.url}, header ${source.acceptHeaderValue} is not acceptable").stackTraceToString())
                harvested == null -> LOGGER.info("Not able to harvest ${source.url}")
                else -> checkHarvestedContainsChanges(harvested, source.url, harvestDate)
            }
        } else LOGGER.error(Exception("Harvest source is not defined").stackTraceToString())

    private fun checkHarvestedContainsChanges(harvested: Model, sourceURL: String, harvestDate: Calendar) {
        val dbData = turtleService
            .getHarvestSource(sourceURL)
            ?.let { parseRDFResponse(it, Lang.TURTLE, null) }

        if (dbData != null && harvested.isIsomorphicWith(dbData)) {
            LOGGER.info("No changes from last harvest of $sourceURL")
        } else {
            LOGGER.info("Changes detected, saving data from $sourceURL and updating FDK meta data")
            turtleService.saveAsHarvestSource(harvested, sourceURL)

            val services = splitServicesFromRDF(harvested)

            if (services.isEmpty()) LOGGER.error(Exception("No public services found in data harvested from $sourceURL").stackTraceToString())
            else updateDB(services, harvestDate)
        }
    }



    private fun updateDB(events: List<PublicServiceRDFModel>, harvestDate: Calendar) {
        events
            .map { Pair(it, metaRepository.findByIdOrNull(it.resourceURI)) }
            .filter { it.first.hasChanges(it.second?.fdkId) }
            .forEach {
                val updatedMeta = it.first.updateMeta(harvestDate, it.second)
                metaRepository.save(updatedMeta)

                turtleService.saveAsPublicService(it.first.harvested, updatedMeta.fdkId, false)

                val fdkUri = "${applicationProperties.publicServiceHarvesterUri}/${updatedMeta.fdkId}"

                val metaModel = ModelFactory.createDefaultModel()
                metaModel.createResource(fdkUri)
                    .addProperty(RDF.type, DCAT.CatalogRecord)
                    .addProperty(DCTerms.identifier, updatedMeta.fdkId)
                    .addProperty(FOAF.primaryTopic, metaModel.createResource(updatedMeta.uri))
                    .addProperty(DCTerms.issued, metaModel.createTypedLiteral(calendarFromTimestamp(updatedMeta.issued)))
                    .addProperty(DCTerms.modified, metaModel.createTypedLiteral(harvestDate))

                turtleService.saveAsPublicService(metaModel.union(it.first.harvested), updatedMeta.fdkId, true)
            }
    }

    private fun PublicServiceRDFModel.updateMeta(
        harvestDate: Calendar,
        dbMeta: PublicServiceMeta?
    ): PublicServiceMeta {
        val fdkId = dbMeta?.fdkId ?: createIdFromUri(resourceURI)
        val issued = dbMeta?.issued
            ?.let { timestamp -> calendarFromTimestamp(timestamp) }
            ?: harvestDate

        return PublicServiceMeta(
            uri = resourceURI,
            fdkId = fdkId,
            issued = issued.timeInMillis,
            modified = harvestDate.timeInMillis
        )
    }

    private fun PublicServiceRDFModel.hasChanges(fdkId: String?): Boolean =
        if (fdkId == null) true
        else harvestDiff(turtleService.getPublicService(fdkId, withRecords = false))
}
