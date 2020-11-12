package no.fdk.fdk_service_harvester.harvester

import no.fdk.fdk_service_harvester.adapter.FusekiAdapter
import no.fdk.fdk_service_harvester.configuration.ApplicationProperties
import no.fdk.fdk_service_harvester.adapter.ServicesAdapter
import no.fdk.fdk_service_harvester.repository.ServicesRepository
import no.fdk.fdk_service_harvester.repository.MiscellaneousRepository
import no.fdk.fdk_service_harvester.service.gzip
import no.fdk.fdk_service_harvester.service.ungzip
import no.fdk.fdk_service_harvester.model.*
import no.fdk.fdk_service_harvester.rdf.*
import org.apache.jena.rdf.model.Model
import org.apache.jena.rdf.model.ModelFactory
import org.apache.jena.sparql.vocabulary.FOAF
import org.apache.jena.vocabulary.DCAT
import org.apache.jena.vocabulary.DCTerms
import org.apache.jena.vocabulary.RDF
import org.slf4j.LoggerFactory
import org.springframework.data.repository.findByIdOrNull
import org.springframework.stereotype.Service
import java.util.*

private val LOGGER = LoggerFactory.getLogger(ServicesHarvester::class.java)

@Service
class ServicesHarvester(
    private val adapter: ServicesAdapter,
    private val fusekiAdapter: FusekiAdapter,
    private val servicesRepository: ServicesRepository,
    private val miscRepository: MiscellaneousRepository,
    private val applicationProperties: ApplicationProperties
) {

    fun updateUnionModel() {
        var unionModel = ModelFactory.createDefaultModel()

        servicesRepository.findAll()
            .map { parseRDFResponse(ungzip(it.turtleService), JenaType.TURTLE, null) }
            .forEach { unionModel = unionModel.union(it) }

        fusekiAdapter.storeUnionModel(unionModel)

        miscRepository.save(
            MiscellaneousTurtle(
                id = UNION_ID,
                isHarvestedSource = false,
                turtle = gzip(unionModel.createRDFResponse(JenaType.TURTLE))
            )
        )
    }

    fun harvestServices(source: HarvestDataSource, harvestDate: Calendar) =
        if (source.url != null) {
            LOGGER.debug("Starting harvest of ${source.url}")
            val jenaWriterType = jenaTypeFromAcceptHeader(source.acceptHeaderValue)

            val harvested = when (jenaWriterType) {
                null -> null
                JenaType.NOT_JENA -> null
                else -> adapter.fetchServices(source)?.let { parseRDFResponse(it, jenaWriterType, source.url) }
            }

            when {
                jenaWriterType == null -> LOGGER.error("Not able to harvest from ${source.url}, no accept header supplied")
                jenaWriterType == JenaType.NOT_JENA -> LOGGER.error("Not able to harvest from ${source.url}, header ${source.acceptHeaderValue} is not acceptable ")
                harvested == null -> LOGGER.info("Not able to harvest ${source.url}")
                else -> checkHarvestedContainsChanges(harvested, source.url, harvestDate)
            }
        } else LOGGER.error("Harvest source is not defined")

    private fun checkHarvestedContainsChanges(harvested: Model, sourceURL: String, harvestDate: Calendar) {
        val dbData = miscRepository
            .findByIdOrNull(sourceURL)
            ?.let { parseRDFResponse(ungzip(it.turtle), JenaType.TURTLE, null) }

        if (dbData != null && harvested.isIsomorphicWith(dbData)) {
            LOGGER.info("No changes from last harvest of $sourceURL")
        } else {
            LOGGER.info("Changes detected, saving data from $sourceURL and updating FDK meta data")
            miscRepository.save(
                MiscellaneousTurtle(
                    id = sourceURL,
                    isHarvestedSource = true,
                    turtle = gzip(harvested.createRDFResponse(JenaType.TURTLE))
                )
            )

            val services = splitServicesFromRDF(harvested)

            if (services.isEmpty()) LOGGER.error("No public services found in data harvested from $sourceURL")
            else updateDB(services, harvestDate)
        }
    }

    private fun updateDB(services: List<PublicServiceRDFModel>, harvestDate: Calendar) {
        val servicesToSave = mutableListOf<ServiceDBO>()

        services
            .map { Pair(it, servicesRepository.findByIdOrNull(it.resource.uri)) }
            .filter { it.first.harvestDiff(it.second) }
            .forEach {
                val serviceURI = it.first.resource.uri

                val fdkId = it.second?.fdkId ?: createIdFromUri(serviceURI)
                val fdkUri = "${applicationProperties.serviceHarvesterUri}/$fdkId"

                val issued = it.second?.issued
                    ?.let { timestamp -> calendarFromTimestamp(timestamp) }
                    ?: harvestDate

                val metaModel = ModelFactory.createDefaultModel()
                metaModel.addMetaPrefixes()

                metaModel.createResource(fdkUri)
                    .addProperty(RDF.type, DCAT.CatalogRecord)
                    .addProperty(DCTerms.identifier, fdkId)
                    .addProperty(FOAF.primaryTopic, metaModel.createResource(serviceURI))
                    .addProperty(DCTerms.issued, metaModel.createTypedLiteral(issued))
                    .addProperty(DCTerms.modified, metaModel.createTypedLiteral(harvestDate))

                val serviceModel = metaModel.union(it.first.harvested)

                servicesToSave.add(
                    ServiceDBO(
                        uri = serviceURI,
                        fdkId = fdkId,
                        issued = issued.timeInMillis,
                        modified = harvestDate.timeInMillis,
                        turtleHarvested = gzip(it.first.harvested.createRDFResponse(JenaType.TURTLE)),
                        turtleService = gzip(serviceModel.createRDFResponse(JenaType.TURTLE))
                    )
                )
            }

        servicesRepository.saveAll(servicesToSave)
    }
}
