package no.fdk.fdk_public_service_harvester.harvester

import no.fdk.fdk_public_service_harvester.rdf.*
import org.apache.jena.query.QueryExecutionFactory
import org.apache.jena.query.QueryFactory
import org.apache.jena.rdf.model.*
import org.apache.jena.riot.Lang
import org.apache.jena.sparql.vocabulary.FOAF
import org.apache.jena.vocabulary.*
import java.util.*


fun PublicServiceRDFModel.harvestDiff(dbTurtle: String?): Boolean =
    if (dbTurtle == null) true
    else !harvested.isIsomorphicWith(parseRDFResponse(dbTurtle, Lang.TURTLE, null))

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
                resourceURI = resource.uri,
                harvested = model
            )
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

    if (resourceShouldBeAdded(resource.uri, types)) {
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

fun Model.addMetaPrefixes(): Model {
    setNsPrefix("dct", DCTerms.NS)
    setNsPrefix("dcat", DCAT.NS)
    setNsPrefix("foaf", FOAF.getURI())
    setNsPrefix("xsd", XSD.NS)

    return this
}

private fun Model.resourceShouldBeAdded(resourceURI: String, types: List<RDFNode>): Boolean =
    when {
        types.contains(CPSV.PublicService) -> false
        containsTriple("<${resourceURI}>", "a", "?o") -> false
        else -> true
    }

private fun Model.containsTriple(subj: String, pred: String, obj: String): Boolean {
    val askQuery = "ASK { $subj $pred $obj }"

    val query = QueryFactory.create(askQuery)
    return QueryExecutionFactory.create(query, this).execAsk()
}
