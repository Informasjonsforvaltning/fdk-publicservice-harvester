package no.fdk.fdk_public_service_harvester.service

import no.fdk.fdk_public_service_harvester.harvester.formatNowWithOsloTimeZone
import no.fdk.fdk_public_service_harvester.model.DuplicateIRI
import no.fdk.fdk_public_service_harvester.model.FdkIdAndUri
import no.fdk.fdk_public_service_harvester.model.HarvestReport
import no.fdk.fdk_public_service_harvester.rabbit.RabbitMQPublisher
import no.fdk.fdk_public_service_harvester.rdf.createRDFResponse
import no.fdk.fdk_public_service_harvester.rdf.parseRDFResponse
import no.fdk.fdk_public_service_harvester.repository.PublicServicesRepository
import org.apache.jena.rdf.model.ModelFactory
import org.apache.jena.riot.Lang
import org.springframework.data.repository.findByIdOrNull
import org.springframework.http.HttpStatus
import org.springframework.stereotype.Service
import org.springframework.web.server.ResponseStatusException

@Service
class PublicServicesService(
    private val servicesRepository: PublicServicesRepository,
    private val rabbitPublisher: RabbitMQPublisher,
    private val turtleService: TurtleService
) {

    fun getAllServices(returnType: Lang, withRecords: Boolean): String =
        turtleService.getServiceUnion(withRecords)
            ?.let {
                if (returnType == Lang.TURTLE) it
                else parseRDFResponse(it, Lang.TURTLE).createRDFResponse(returnType)
            }
            ?: ModelFactory.createDefaultModel().createRDFResponse(returnType)

    fun getServiceById(id: String, returnType: Lang, withRecords: Boolean): String? =
        turtleService.getPublicService(id, withRecords)
            ?.let {
                if (returnType == Lang.TURTLE) it
                else parseRDFResponse(it, Lang.TURTLE).createRDFResponse(returnType)
            }

    fun getCatalogs(returnType: Lang, withRecords: Boolean): String =
        turtleService.getCatalogUnion(withRecords)
            ?.let {
                if (returnType == Lang.TURTLE) it
                else parseRDFResponse(it, Lang.TURTLE).createRDFResponse(returnType)
            }
            ?: ModelFactory.createDefaultModel().createRDFResponse(returnType)

    fun getCatalogById(id: String, returnType: Lang, withRecords: Boolean): String? =
        turtleService.getCatalog(id, withRecords)
            ?.let {
                if (returnType == Lang.TURTLE) it
                else parseRDFResponse(it, Lang.TURTLE).createRDFResponse(returnType)
            }

    fun removeService(id: String) {
        val start = formatNowWithOsloTimeZone()
        val meta = servicesRepository.findAllByFdkId(id)
        if (meta.isEmpty()) {
            throw ResponseStatusException(HttpStatus.NOT_FOUND, "No service found with fdkID $id")
        } else if (meta.none { !it.removed }) {
            throw ResponseStatusException(HttpStatus.BAD_REQUEST, "Service with fdkID $id has already been removed")
        } else {
            servicesRepository.saveAll(meta.map { it.copy(removed = true) })

            val uri = meta.first().uri
            rabbitPublisher.send(listOf(
                HarvestReport(
                    id = "manual-delete-$id",
                    url = uri,
                    harvestError = false,
                    startTime = start,
                    endTime = formatNowWithOsloTimeZone(),
                    removedResources = listOf(FdkIdAndUri(fdkId = id, uri = uri))
                )
            ))
        }
    }

    fun removeDuplicates(duplicates: List<DuplicateIRI>) {
        val start = formatNowWithOsloTimeZone()
        val reportAsRemoved: MutableList<FdkIdAndUri> = mutableListOf()

        duplicates.flatMap { duplicate ->
            val remove = servicesRepository.findByIdOrNull(duplicate.iriToRemove)
                ?: throw ResponseStatusException(HttpStatus.BAD_REQUEST, "No service connected to IRI ${duplicate.iriToRemove}")

            val retain = servicesRepository.findByIdOrNull(duplicate.iriToRetain)
                ?.let { if (it.issued > remove.issued) it.copy(issued = remove.issued) else it } // keep earliest issued
                ?.let { if (it.modified < remove.modified) it.copy(modified = remove.modified) else it } // keep latest modified
                ?.let {
                    if (duplicate.keepRemovedFdkId) {
                        if (it.removed) throw ResponseStatusException(HttpStatus.BAD_REQUEST, "Service with IRI ${it.uri} has already been removed")
                        reportAsRemoved.add(FdkIdAndUri(fdkId = it.fdkId, uri = it.uri))
                        it.copy(fdkId = remove.fdkId)
                    } else {
                        if (remove.removed) throw ResponseStatusException(HttpStatus.BAD_REQUEST, "Service with IRI ${remove.uri} has already been removed")
                        reportAsRemoved.add(FdkIdAndUri(fdkId = remove.fdkId, uri = remove.uri))
                        it
                    }
                }
                ?: remove.copy(uri = duplicate.iriToRetain)

            listOf(remove.copy(removed = true), retain.copy(removed = false))
        }.run { servicesRepository.saveAll(this) }

        if (reportAsRemoved.isNotEmpty()) {
            rabbitPublisher.send(listOf(
                HarvestReport(
                    id = "duplicate-delete",
                    url = "https://fellesdatakatalog.digdir.no/duplicates",
                    harvestError = false,
                    startTime = start,
                    endTime = formatNowWithOsloTimeZone(),
                    removedResources = reportAsRemoved
                )
            ))
        }
    }

    // Purges everything associated with a removed fdkID
    fun purgeByFdkId(fdkId: String) {
        servicesRepository.findAllByFdkId(fdkId)
            .also { services -> if (services.any { !it.removed }) throw ResponseStatusException(HttpStatus.BAD_REQUEST, "Unable to purge files, service with id $fdkId has not been removed") }
            .run { servicesRepository.deleteAll(this) }

        turtleService.deleteServiceFiles(fdkId)
    }

}
