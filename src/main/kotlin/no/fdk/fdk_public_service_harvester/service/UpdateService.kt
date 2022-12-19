package no.fdk.fdk_public_service_harvester.service

import no.fdk.fdk_public_service_harvester.configuration.ApplicationProperties
import no.fdk.fdk_public_service_harvester.harvester.calendarFromTimestamp
import no.fdk.fdk_public_service_harvester.model.*
import no.fdk.fdk_public_service_harvester.rdf.DCATNO
import no.fdk.fdk_public_service_harvester.rdf.containsTriple
import no.fdk.fdk_public_service_harvester.rdf.parseRDFResponse
import no.fdk.fdk_public_service_harvester.rdf.safeAddProperty
import no.fdk.fdk_public_service_harvester.repository.CatalogRepository
import no.fdk.fdk_public_service_harvester.repository.PublicServicesRepository
import org.apache.jena.rdf.model.Model
import org.apache.jena.rdf.model.ModelFactory
import org.apache.jena.riot.Lang
import org.apache.jena.sparql.vocabulary.FOAF
import org.apache.jena.vocabulary.DCAT
import org.apache.jena.vocabulary.DCTerms
import org.apache.jena.vocabulary.RDF
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service

private val LOGGER = LoggerFactory.getLogger(UpdateService::class.java)

@Service
class UpdateService (
    private val applicationProperties: ApplicationProperties,
    private val serviceMetaRepository: PublicServicesRepository,
    private val catalogMetaRepository: CatalogRepository,
    private val turtleService: TurtleService
) {

    fun updateUnionModels() {
        var serviceUnion = ModelFactory.createDefaultModel()
        var serviceUnionNoRecords = ModelFactory.createDefaultModel()

        serviceMetaRepository.findAll()
            .forEach {
                turtleService.getPublicService(it.fdkId, withRecords = true)
                    ?.let { dboTurtle -> parseRDFResponse(dboTurtle, Lang.TURTLE, null) }
                    ?.run { serviceUnion = serviceUnion.union(this) }

                turtleService.getPublicService(it.fdkId, withRecords = false)
                    ?.let { dboTurtle -> parseRDFResponse(dboTurtle, Lang.TURTLE, null) }
                    ?.run { serviceUnionNoRecords = serviceUnionNoRecords.union(this) }
            }

        turtleService.saveAsServiceUnion(serviceUnion, true)
        turtleService.saveAsServiceUnion(serviceUnionNoRecords, false)

        var catalogUnion = ModelFactory.createDefaultModel()
        var catalogUnionNoRecords = ModelFactory.createDefaultModel()

        catalogMetaRepository.findAll()
            .filter { it.services.isNotEmpty() }
            .forEach {
                turtleService.getCatalog(it.fdkId, withRecords = true)
                    ?.let { turtle -> parseRDFResponse(turtle, Lang.TURTLE, null) }
                    ?.run { catalogUnion = catalogUnion.union(this) }

                turtleService.getCatalog(it.fdkId, withRecords = false)
                    ?.let { turtle -> parseRDFResponse(turtle, Lang.TURTLE, null) }
                    ?.run { catalogUnionNoRecords = catalogUnionNoRecords.union(this) }
            }

        turtleService.saveAsCatalogUnion(catalogUnion, true)
        turtleService.saveAsCatalogUnion(catalogUnionNoRecords, false)
    }

    fun updateMetaData() {
        serviceMetaRepository.findAll()
            .forEach { service ->
                val serviceMeta = service.createMetaModel()

                turtleService.getPublicService(service.fdkId, withRecords = false)
                    ?.let { serviceNoRecords -> parseRDFResponse(serviceNoRecords, Lang.TURTLE, null) }
                    ?.let { serviceModelNoRecords -> serviceMeta.union(serviceModelNoRecords) }
                    ?.run { turtleService.saveAsPublicService(this, fdkId = service.fdkId, withRecords = true) }
            }

        catalogMetaRepository.findAll()
            .forEach { catalog ->
                val catalogNoRecords = turtleService.getCatalog(catalog.fdkId, withRecords = false)
                    ?.let { parseRDFResponse(it, Lang.TURTLE, null) }

                if (catalogNoRecords != null) {
                    val fdkCatalogURI = "${applicationProperties.publicServiceHarvesterUri}/catalogs/${catalog.fdkId}"
                    var catalogMeta = catalog.createMetaModel()

                    serviceMetaRepository.findAllByIsPartOf(fdkCatalogURI)
                        .filter { it.catalogContainsService(catalog.uri, catalogNoRecords) }
                        .forEach { service ->
                            val serviceMeta = service.createMetaModel()
                            catalogMeta = catalogMeta.union(serviceMeta)
                        }

                    turtleService.saveAsCatalog(
                        catalogMeta.union(catalogNoRecords),
                        fdkId = catalog.fdkId,
                        withRecords = true
                    )
                }
            }

        updateUnionModels()
    }

    private fun CatalogMeta.createMetaModel(): Model {
        val fdkUri = "${applicationProperties.publicServiceHarvesterUri}/catalogs/$fdkId"

        val metaModel = ModelFactory.createDefaultModel()

        metaModel.createResource(fdkUri)
            .addProperty(RDF.type, DCAT.CatalogRecord)
            .addProperty(DCTerms.identifier, fdkId)
            .addProperty(FOAF.primaryTopic, metaModel.createResource(uri))
            .addProperty(DCTerms.issued, metaModel.createTypedLiteral(calendarFromTimestamp(issued)))
            .addProperty(DCTerms.modified, metaModel.createTypedLiteral(calendarFromTimestamp(modified)))

        return metaModel
    }

    private fun PublicServiceMeta.createMetaModel(): Model {
        val fdkUri = "${applicationProperties.publicServiceHarvesterUri}/$fdkId"

        val metaModel = ModelFactory.createDefaultModel()

        metaModel.createResource(fdkUri)
            .addProperty(RDF.type, DCAT.CatalogRecord)
            .addProperty(DCTerms.identifier, fdkId)
            .addProperty(FOAF.primaryTopic, metaModel.createResource(uri))
            .safeAddProperty(DCTerms.isPartOf, isPartOf)
            .addProperty(DCTerms.issued, metaModel.createTypedLiteral(calendarFromTimestamp(issued)))
            .addProperty(DCTerms.modified, metaModel.createTypedLiteral(calendarFromTimestamp(modified)))

        return metaModel
    }

    private fun PublicServiceMeta.catalogContainsService(catalogURI: String, catalogModel: Model): Boolean =
        catalogModel.containsTriple("<$catalogURI>", "<${DCATNO.containsService.uri}>", "<$uri>")
}
