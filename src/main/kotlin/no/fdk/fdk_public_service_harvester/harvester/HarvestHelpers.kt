package no.fdk.fdk_public_service_harvester.harvester

import no.fdk.fdk_public_service_harvester.model.PublicServiceDBO
import no.fdk.fdk_public_service_harvester.rdf.*
import no.fdk.fdk_public_service_harvester.service.ungzip
import org.apache.jena.rdf.model.Model
import org.apache.jena.rdf.model.Resource
import org.apache.jena.rdf.model.ResourceRequiredException
import org.apache.jena.rdf.model.Statement
import org.apache.jena.sparql.vocabulary.FOAF
import org.apache.jena.vocabulary.*
import java.util.*


fun PublicServiceRDFModel.harvestDiff(dbo: PublicServiceDBO?): Boolean =
    if (dbo == null) true
    else !harvested.isIsomorphicWith(parseRDFResponse(ungzip(dbo.turtleHarvested), JenaType.TURTLE, null))

fun splitServicesFromRDF(harvested: Model): List<PublicServiceRDFModel> =
    harvested.listResourcesWithProperty(RDF.type, CPSV.PublicService)
        .toList()
        .map { resource ->

            var model = resource.listProperties().toModel()
            model.setNsPrefixes(harvested.nsPrefixMap)

            resource.listProperties().toList()
                .filter { it.isResourceProperty() }
                .forEach {
                    model = model.recursiveAddNonPublicServiceResources(it.resource, 10)
                }

            PublicServiceRDFModel(
                resource = resource,
                harvested = model
            )
        }

private fun Statement.isResourceProperty(): Boolean =
    try {
        resource.isResource
    } catch (ex: ResourceRequiredException) {
        false
    }

private fun Model.recursiveAddNonPublicServiceResources(resource: Resource, recursiveCount: Int): Model {
    val newCount = recursiveCount - 1
    val types = resource.listProperties(RDF.type)
        .toList()
        .map { it.`object` }

    if (!types.contains(CPSV.PublicService)) {

        add(resource.listProperties())

        if (newCount > 0) {
            resource.listProperties().toList()
                .filter { it.isResourceProperty() }
                .forEach { recursiveAddNonPublicServiceResources(it.resource, newCount) }
        }
    }

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
    val resource: Resource,
    val harvested: Model
)

fun Model.addMetaPrefixes(): Model {
    setNsPrefix("dct", DCTerms.NS)
    setNsPrefix("dcat", DCAT.NS)
    setNsPrefix("foaf", FOAF.getURI())
    setNsPrefix("xsd", XSD.NS)

    return this
}