package no.fdk.fdk_public_service_harvester.service

import no.fdk.fdk_public_service_harvester.configuration.ApplicationProperties
import no.fdk.fdk_public_service_harvester.harvester.calendarFromTimestamp
import no.fdk.fdk_public_service_harvester.model.*
import no.fdk.fdk_public_service_harvester.rdf.parseRDFResponse
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
    private val metaRepository: PublicServicesRepository,
    private val turtleService: TurtleService
) {

    fun updateUnionModel() {
        var unionModel = ModelFactory.createDefaultModel()
        var unionModelNoRecords = ModelFactory.createDefaultModel()

        metaRepository.findAll()
            .forEach {
                turtleService.getPublicService(it.fdkId, withRecords = true)
                    ?.let { dboTurtle -> parseRDFResponse(dboTurtle, Lang.TURTLE, null) }
                    ?.run { unionModel = unionModel.union(this) }

                turtleService.getPublicService(it.fdkId, withRecords = false)
                    ?.let { dboTurtle -> parseRDFResponse(dboTurtle, Lang.TURTLE, null) }
                    ?.run { unionModelNoRecords = unionModelNoRecords.union(this) }
            }

        turtleService.saveAsUnion(unionModel, true)
        turtleService.saveAsUnion(unionModelNoRecords, false)
    }

    fun updateMetaData() {
        LOGGER.info("Updating catalog records for all public services.")
        metaRepository.findAll()
            .forEach { event ->
                val catalogMeta = event.createMetaModel()

                turtleService.getPublicService(event.fdkId, withRecords = false)
                    ?.let { eventNoRecords -> parseRDFResponse(eventNoRecords, Lang.TURTLE, null) }
                    ?.let { eventModelNoRecords -> catalogMeta.union(eventModelNoRecords) }
                    ?.run { turtleService.saveAsPublicService(this, event.fdkId, withRecords = true) }
            }

        updateUnionModel()
    }

    private fun PublicServiceMeta.createMetaModel(): Model {
        val fdkUri = "${applicationProperties.publicServiceHarvesterUri}/$fdkId"

        val metaModel = ModelFactory.createDefaultModel()

        metaModel.createResource(fdkUri)
            .addProperty(RDF.type, DCAT.CatalogRecord)
            .addProperty(DCTerms.identifier, fdkId)
            .addProperty(FOAF.primaryTopic, metaModel.createResource(uri))
            .addProperty(DCTerms.issued, metaModel.createTypedLiteral(calendarFromTimestamp(issued)))
            .addProperty(DCTerms.modified, metaModel.createTypedLiteral(calendarFromTimestamp(modified)))

        return metaModel
    }
}
