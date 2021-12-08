package no.fdk.fdk_public_service_harvester.harvester

import no.fdk.fdk_public_service_harvester.Application
import no.fdk.fdk_public_service_harvester.rdf.*
import org.apache.jena.query.QueryExecutionFactory
import org.apache.jena.query.QueryFactory
import org.apache.jena.rdf.model.*
import org.apache.jena.riot.Lang
import org.apache.jena.sparql.vocabulary.FOAF
import org.apache.jena.vocabulary.*
import org.slf4j.LoggerFactory
import java.util.*

private val LOGGER = LoggerFactory.getLogger(Application::class.java)

fun PublicServiceRDFModel.harvestDiff(dbTurtle: String?): Boolean =
    if (dbTurtle == null) true
    else !harvested.isIsomorphicWith(parseRDFResponse(dbTurtle, Lang.TURTLE, null))

fun splitServicesFromRDF(harvested: Model, sourceURL: String): List<PublicServiceRDFModel> =
    harvested.listResourcesWithProperty(RDF.type, CPSV.PublicService)
        .toList()
        .filterBlankNodeServices(sourceURL)
        .map { resource ->

            var model = resource.listProperties().toModel()
            model.setNsPrefixes(harvested.nsPrefixMap)

            resource.listProperties().toList()
                .filter { it.isResourceProperty() }
                .forEach {
                    model = model.recursiveAddNonPublicServiceResources(it.resource, 10)
                }

            PublicServiceRDFModel(
                resourceURI = resource.uri,
                harvested = model
            )
        }

private fun List<Resource>.filterBlankNodeServices(sourceURL: String): List<Resource> =
    filter {
        if (it.isURIResource) true
        else {
            LOGGER.error(
                "Failed harvest of service for $sourceURL, unable to harvest blank node services",
                Exception("unable to harvest blank node services")
            )
            false
        }
    }

private fun Statement.isResourceProperty(): Boolean =
    try {
        resource.isResource
    } catch (ex: ResourceRequiredException) {
        false
    }

private fun Model.addAgentsAssociatedWithParticipation(resource: Resource): Model {
    resource.model
        .listResourcesWithProperty(RDF.type, DCTerms.Agent)
        .toList()
        .filter { it.hasProperty(CV.playsRole, resource) }
        .forEach { codeElement ->
            add(codeElement.listProperties())

            codeElement.listProperties().toList()
                .filter { it.isResourceProperty() }
                .forEach { add(it.resource.listProperties()) }
        }

    return this
}

private fun Model.recursiveAddNonPublicServiceResources(resource: Resource, recursiveCount: Int): Model {
    val newCount = recursiveCount - 1
    val types = resource.listProperties(RDF.type)
        .toList()
        .map { it.`object` }

    if (resourceShouldBeAdded(resource, types)) {
        add(resource.listProperties())

        if (newCount > 0) {
            resource.listProperties().toList()
                .filter { it.isResourceProperty() }
                .forEach { recursiveAddNonPublicServiceResources(it.resource, newCount) }
        }
    }

    if (types.contains(CV.Participation)) addAgentsAssociatedWithParticipation(resource)

    return this
}

fun calendarFromTimestamp(timestamp: Long): Calendar {
    val calendar = Calendar.getInstance()
    calendar.timeInMillis = timestamp
    return calendar
}

fun createIdFromUri(uri: String): String =
    UUID.nameUUIDFromBytes(uri.toByteArray())
        .toString()

data class PublicServiceRDFModel (
    val resourceURI: String,
    val harvested: Model
)

private fun Model.resourceShouldBeAdded(resource: Resource, types: List<RDFNode>): Boolean =
    when {
        types.contains(CPSV.PublicService) -> false
        types.contains(CV.BusinessEvent) -> false
        types.contains(CV.LifeEvent) -> false
        !resource.isURIResource -> true
        containsTriple("<${resource.uri}>", "a", "?o") -> false
        else -> true
    }

private fun Model.containsTriple(subj: String, pred: String, obj: String): Boolean {
    val askQuery = "ASK { $subj $pred $obj }"

    return try {
        val query = QueryFactory.create(askQuery)
        return QueryExecutionFactory.create(query, this).execAsk()
    } catch (ex: Exception) { false }
}

class HarvestException(url: String) : Exception("Harvest failed for $url")
