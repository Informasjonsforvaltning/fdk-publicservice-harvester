package no.fdk.fdk_public_service_harvester.harvester

import no.fdk.fdk_public_service_harvester.adapter.OrganizationsAdapter
import no.fdk.fdk_public_service_harvester.configuration.ApplicationProperties
import no.fdk.fdk_public_service_harvester.adapter.ServicesAdapter
import no.fdk.fdk_public_service_harvester.repository.PublicServicesRepository
import no.fdk.fdk_public_service_harvester.model.*
import no.fdk.fdk_public_service_harvester.rdf.*
import no.fdk.fdk_public_service_harvester.repository.CatalogRepository
import no.fdk.fdk_public_service_harvester.service.TurtleService
import org.apache.jena.rdf.model.Model
import org.apache.jena.riot.Lang
import org.slf4j.LoggerFactory
import org.springframework.data.repository.findByIdOrNull
import org.springframework.stereotype.Service
import java.util.*

private val LOGGER = LoggerFactory.getLogger(PublicServicesHarvester::class.java)

@Service
class PublicServicesHarvester(
    private val adapter: ServicesAdapter,
    private val orgAdapter: OrganizationsAdapter,
    private val serviceMetaRepository: PublicServicesRepository,
    private val catalogMetaRepository: CatalogRepository,
    private val turtleService: TurtleService,
    private val applicationProperties: ApplicationProperties
) {

    fun harvestServices(source: HarvestDataSource, harvestDate: Calendar, forceUpdate: Boolean): HarvestReport? =
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
                        parseRDFResponse(adapter.fetchServices(source), jenaWriterType),
                        source.id, source.url, harvestDate, source.publisherId, forceUpdate
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

    private fun updateIfChanged(harvested: Model, sourceId: String, sourceURL: String, harvestDate: Calendar,
                                publisherId: String?, forceUpdate: Boolean): HarvestReport {
        val dbData = turtleService
            .getHarvestSource(sourceURL)
            ?.let { safeParseRDF(it, Lang.TURTLE) }

        return if (!forceUpdate && dbData != null && harvested.isIsomorphicWith(dbData)) {
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

            updateDB(harvested, sourceId, sourceURL, harvestDate, publisherId, forceUpdate)
        }
    }

    private fun updateDB(harvested: Model, sourceId: String, sourceURL: String, harvestDate: Calendar,
                         publisherId: String?, forceUpdate: Boolean): HarvestReport {
        val allServices = splitServicesFromRDF(harvested, sourceURL)
        val updatedServices = updateServices(allServices, harvestDate, forceUpdate)

        val organization = if (publisherId != null && allServices.containsFreeServices()) {
            orgAdapter.getOrganization(publisherId)
        } else null

        val catalogs = splitCatalogsFromRDF(harvested, allServices, sourceURL, organization)
        val updatedCatalogs = updateCatalogs(catalogs, harvestDate, forceUpdate)

        val removedServices = getServicesRemovedThisHarvest(
            updatedCatalogs.map { catalogFdkUri(it.fdkId) },
            allServices.map { it.resourceURI }
        )
        removedServices.map { it.copy(removed = true) }
            .run { serviceMetaRepository.saveAll(this) }

        return HarvestReport(
            id = sourceId,
            url = sourceURL,
            harvestError = false,
            startTime = harvestDate.formatWithOsloTimeZone(),
            endTime = formatNowWithOsloTimeZone(),
            changedCatalogs = updatedCatalogs,
            changedResources = updatedServices,
            removedResources = removedServices.map { FdkIdAndUri(fdkId = it.fdkId, uri = it.uri) }
        )
    }

    private fun updateCatalogs(catalogs: List<CatalogRDFModel>, harvestDate: Calendar, forceUpdate: Boolean): List<FdkIdAndUri> =
        catalogs
            .map { Pair(it, catalogMetaRepository.findByIdOrNull(it.resourceURI)) }
            .filter { forceUpdate || it.first.hasChanges(it.second?.fdkId) }
            .map {
                val updatedMeta = it.first.mapToMetaDBO(harvestDate, it.second)
                catalogMetaRepository.save(updatedMeta)

                turtleService.saveAsCatalog(
                    model = it.first.harvested,
                    fdkId = updatedMeta.fdkId,
                    withRecords = false
                )

                it.first.services.forEach { serviceURI -> addIsPartOfToService(serviceURI, catalogFdkUri(updatedMeta.fdkId)) }

                FdkIdAndUri(fdkId = updatedMeta.fdkId, uri = updatedMeta.uri)
            }

    private fun CatalogRDFModel.mapToMetaDBO(
        harvestDate: Calendar,
        dbMeta: CatalogMeta?
    ): CatalogMeta {
        val catalogURI = resourceURI
        val fdkId = dbMeta?.fdkId ?: createIdFromString(catalogURI)
        val issued = dbMeta?.issued
            ?.let { timestamp -> calendarFromTimestamp(timestamp) }
            ?: harvestDate

        return CatalogMeta(
            uri = catalogURI,
            fdkId = fdkId,
            issued = issued.timeInMillis,
            modified = harvestDate.timeInMillis,
            services = services
        )
    }

    private fun updateServices(services: List<PublicServiceRDFModel>, harvestDate: Calendar, forceUpdate: Boolean): List<FdkIdAndUri> =
        services.mapNotNull {
            it.updateDBOs(harvestDate, forceUpdate)
                ?.let { meta -> FdkIdAndUri(fdkId = meta.fdkId, uri = it.resourceURI) }
        }

    private fun PublicServiceRDFModel.updateDBOs(harvestDate: Calendar, forceUpdate: Boolean): PublicServiceMeta? {
        val dbMeta = serviceMetaRepository.findByIdOrNull(resourceURI)
        return when {
            dbMeta == null || dbMeta.removed || hasChanges(dbMeta.fdkId) -> {
                val updatedMeta = mapToMetaDBO(harvestDate, dbMeta)
                serviceMetaRepository.save(updatedMeta)
                turtleService.saveAsPublicService(harvested, updatedMeta.fdkId, false)

                updatedMeta
            }
            forceUpdate -> {
                turtleService.saveAsPublicService(
                    model = harvested,
                    fdkId = dbMeta.fdkId,
                    withRecords = false
                )
                dbMeta
            }
            else -> null
        }
    }

    private fun PublicServiceRDFModel.mapToMetaDBO(
        harvestDate: Calendar,
        dbMeta: PublicServiceMeta?
    ): PublicServiceMeta {
        val fdkId = dbMeta?.fdkId ?: createIdFromString(resourceURI)
        val issued = dbMeta?.issued
            ?.let { timestamp -> calendarFromTimestamp(timestamp) }
            ?: harvestDate

        return PublicServiceMeta(
            uri = resourceURI,
            fdkId = fdkId,
            isPartOf = dbMeta?.isPartOf,
            issued = issued.timeInMillis,
            modified = harvestDate.timeInMillis
        )
    }

    private fun catalogFdkUri(fdkId: String): String =
        "${applicationProperties.publicServiceHarvesterUri}/catalogs/$fdkId"

    private fun getServicesRemovedThisHarvest(catalogs: List<String>, services: List<String>): List<PublicServiceMeta> =
        catalogs.flatMap { serviceMetaRepository.findAllByIsPartOf(it) }
            .filter { !it.removed && !services.contains(it.uri) }

    private fun addIsPartOfToService(serviceURI: String, catalogURI: String) =
        serviceMetaRepository.findByIdOrNull(serviceURI)
            ?.run { if (isPartOf != catalogURI) serviceMetaRepository.save(copy(isPartOf = catalogURI)) }

    private fun PublicServiceRDFModel.hasChanges(fdkId: String?): Boolean =
        if (fdkId == null) true
        else harvestDiff(turtleService.getPublicService(fdkId, withRecords = false))

    private fun CatalogRDFModel.hasChanges(fdkId: String?): Boolean =
        if (fdkId == null) true
        else harvestDiff(turtleService.getCatalog(fdkId, withRecords = false))
}
