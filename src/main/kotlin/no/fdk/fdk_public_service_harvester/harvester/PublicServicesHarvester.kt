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
import java.time.ZoneId
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.util.*

private val LOGGER = LoggerFactory.getLogger(PublicServicesHarvester::class.java)
private const val dateFormat: String = "yyyy-MM-dd HH:mm:ss Z"

@Service
class PublicServicesHarvester(
    private val adapter: ServicesAdapter,
    private val metaRepository: PublicServicesRepository,
    private val turtleService: TurtleService,
    private val applicationProperties: ApplicationProperties
) {

    fun harvestServices(source: HarvestDataSource, harvestDate: Calendar): HarvestReport? =
        if (source.id != null && source.url != null) {
            try {
                LOGGER.debug("Starting harvest of ${source.url}")

                when (val jenaWriterType = jenaTypeFromAcceptHeader(source.acceptHeaderValue)) {
                    null -> {
                        LOGGER.error(
                            "Not able to harvest from ${source.url}, no accept header supplied",
                            HarvestException(source.url)
                        )
                        HarvestReport(
                            id = source.id,
                            url = source.url,
                            harvestError = true,
                            errorMessage = "Not able to harvest, no accept header supplied",
                            startTime = harvestDate.formatWithOsloTimeZone(),
                            endTime = formatNowWithOsloTimeZone()
                        )
                    }
                    Lang.RDFNULL -> {
                        LOGGER.error(
                            "Not able to harvest from ${source.url}, header ${source.acceptHeaderValue} is not acceptable",
                            HarvestException(source.url)
                        )
                        HarvestReport(
                            id = source.id,
                            url = source.url,
                            harvestError = true,
                            errorMessage = "Not able to harvest, no accept header supplied",
                            startTime = harvestDate.formatWithOsloTimeZone(),
                            endTime = formatNowWithOsloTimeZone()
                        )
                    }
                    else -> updateIfChanged(
                        parseRDFResponse(adapter.fetchServices(source), jenaWriterType, source.url),
                        source.id, source.url, harvestDate
                    )
                }
            } catch (ex: Exception) {
                LOGGER.error("Harvest of ${source.url} failed", ex)
                HarvestReport(
                    id = source.id,
                    url = source.url,
                    harvestError = true,
                    errorMessage = ex.message,
                    startTime = harvestDate.formatWithOsloTimeZone(),
                    endTime = formatNowWithOsloTimeZone()
                )
            }
        } else {
            LOGGER.error("Harvest source is not defined", HarvestException("undefined"))
            null
        }

    private fun updateIfChanged(harvested: Model, sourceId: String, sourceURL: String, harvestDate: Calendar): HarvestReport {
        val dbData = turtleService
            .getHarvestSource(sourceURL)
            ?.let { parseRDFResponse(it, Lang.TURTLE, null) }

        return if (dbData != null && harvested.isIsomorphicWith(dbData)) {
            LOGGER.info("No changes from last harvest of $sourceURL")
            HarvestReport(
                id = sourceId,
                url = sourceURL,
                harvestError = false,
                startTime = harvestDate.formatWithOsloTimeZone(),
                endTime = formatNowWithOsloTimeZone()
            )
        } else {
            LOGGER.info("Changes detected, saving data from $sourceURL and updating FDK meta data")
            turtleService.saveAsHarvestSource(harvested, sourceURL)

            updateDB(harvested, sourceId, sourceURL, harvestDate)
        }
    }



    private fun updateDB(harvested: Model, sourceId: String, sourceURL: String, harvestDate: Calendar): HarvestReport {
        val updatedServices = mutableListOf<PublicServiceMeta>()
        splitServicesFromRDF(harvested, sourceURL)
            .map { Pair(it, metaRepository.findByIdOrNull(it.resourceURI)) }
            .filter { it.first.hasChanges(it.second?.fdkId) }
            .forEach {
                val updatedMeta = it.first.updateMeta(harvestDate, it.second)
                metaRepository.save(updatedMeta)
                updatedServices.add(updatedMeta)

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

        return HarvestReport(
            id = sourceId,
            url = sourceURL,
            harvestError = false,
            startTime = harvestDate.formatWithOsloTimeZone(),
            endTime = formatNowWithOsloTimeZone(),
            changedResources = updatedServices.map { FdkIdAndUri(fdkId = it.fdkId, uri = it.uri) }
        )
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

    private fun formatNowWithOsloTimeZone(): String =
        ZonedDateTime.now(ZoneId.of("Europe/Oslo"))
            .format(DateTimeFormatter.ofPattern(dateFormat))

    private fun Calendar.formatWithOsloTimeZone(): String =
        ZonedDateTime.from(toInstant().atZone(ZoneId.of("Europe/Oslo")))
            .format(DateTimeFormatter.ofPattern(dateFormat))
}
