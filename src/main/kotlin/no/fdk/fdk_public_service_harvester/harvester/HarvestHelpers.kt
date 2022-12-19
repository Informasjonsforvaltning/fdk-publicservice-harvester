package no.fdk.fdk_public_service_harvester.harvester

import no.fdk.fdk_public_service_harvester.Application
import no.fdk.fdk_public_service_harvester.rdf.*
import org.apache.jena.query.QueryExecutionFactory
import org.apache.jena.query.QueryFactory
import org.apache.jena.rdf.model.*
import org.apache.jena.riot.Lang
import org.apache.jena.vocabulary.*
import org.slf4j.LoggerFactory
import java.util.*

private val LOGGER = LoggerFactory.getLogger(Application::class.java)

fun CatalogRDFModel.harvestDiff(dbTurtle: String?): Boolean =
    if (dbTurtle == null) true
    else !harvested.isIsomorphicWith(parseRDFResponse(dbTurtle, Lang.TURTLE, null))

fun PublicServiceRDFModel.harvestDiff(dbTurtle: String?): Boolean =
    if (dbTurtle == null) true
    else !harvested.isIsomorphicWith(parseRDFResponse(dbTurtle, Lang.TURTLE, null))

fun splitCatalogsFromRDF(harvested: Model, allServices: List<PublicServiceRDFModel>, sourceURL: String): List<CatalogRDFModel> =
    harvested.listResourcesWithProperty(RDF.type, DCAT.Catalog)
        .toList()
        .excludeBlankNodes(sourceURL)
        .filter { it.hasProperty(DCATNO.containsService) }
        .map { resource ->
            val catalogServices: Set<String> = resource.listProperties(DCATNO.containsService)
                .toList()
                .filter { it.isResourceProperty() }
                .map { it.resource }
                .excludeBlankNodes(sourceURL)
                .map { it.uri }
                .toSet()

            val catalogModelWithoutServices = resource.extractCatalogModel()

            var catalogModel = catalogModelWithoutServices
            allServices.filter { catalogServices.contains(it.resourceURI) }
                .forEach { catalogModel = catalogModel.union(it.harvested) }

            CatalogRDFModel(
                resourceURI = resource.uri,
                harvestedWithoutServices = catalogModelWithoutServices,
                harvested = catalogModel,
                services = catalogServices
            )
        }

fun splitServicesFromRDF(harvested: Model, sourceURL: String): List<PublicServiceRDFModel> =
    harvested.listResourcesWithServiceType()
        .toList()
        .excludeBlankNodes(sourceURL)
        .map { serviceResource -> serviceResource.extractService() }

fun Resource.extractCatalogModel(): Model {
    val catalogModelWithoutServices = ModelFactory.createDefaultModel()
    catalogModelWithoutServices.setNsPrefixes(model.nsPrefixMap)

    listProperties()
        .toList()
        .forEach { catalogModelWithoutServices.addCatalogProperties(it) }

    return catalogModelWithoutServices
}

fun Resource.extractService(): PublicServiceRDFModel {
    var serviceModel = listProperties().toModel()
    serviceModel = serviceModel.setNsPrefixes(model.nsPrefixMap)

    listProperties().toList()
        .filter { it.isResourceProperty() }
        .forEach { serviceModel = serviceModel.recursiveAddNonPublicServiceResources(it.resource, 10) }

    return PublicServiceRDFModel(
        resourceURI = uri,
        harvested = serviceModel,
        isMemberOfAnyCatalog = isMemberOfAnyCatalog()
    )
}

private fun Model.addCatalogProperties(property: Statement): Model =
    when {
        property.predicate != DCATNO.containsService && property.isResourceProperty() ->
            add(property).recursiveAddNonPublicServiceResources(property.resource, 5)
        property.predicate != DCATNO.containsService -> add(property)
        property.isResourceProperty() && property.resource.isURIResource -> add(property)
        else -> this
    }

private fun Model.listResourcesWithServiceType(): List<Resource> {
    val publicServices = listResourcesWithProperty(RDF.type, CPSV.PublicService)
        .toList()

    val cpsvnoServices = listResourcesWithProperty(RDF.type, CPSVNO.Service)
        .toList()

    return listOf(publicServices, cpsvnoServices).flatten()
}

private fun List<Resource>.excludeBlankNodes(sourceURL: String): List<Resource> =
    filter {
        if (it.isURIResource) true
        else {
            LOGGER.warn("Blank node service or catalog filtered when harvesting $sourceURL")
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
    val harvested: Model,
    val isMemberOfAnyCatalog: Boolean
)

data class CatalogRDFModel(
    val resourceURI: String,
    val harvested: Model,
    val harvestedWithoutServices: Model,
    val services: Set<String>,
)

private fun Model.resourceShouldBeAdded(resource: Resource, types: List<RDFNode>): Boolean =
    when {
        types.contains(CPSV.PublicService) -> false
        types.contains(CPSVNO.Service) -> false
        types.contains(CV.Event) -> false
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

private fun Resource.isMemberOfAnyCatalog(): Boolean {
    val askQuery = """ASK {
        ?catalog a <${DCAT.Catalog.uri}> .
        ?catalog <${DCATNO.containsService.uri}> <$uri> .
    }""".trimMargin()

    val query = QueryFactory.create(askQuery)
    return QueryExecutionFactory.create(query, model).execAsk()
}

class HarvestException(url: String) : Exception("Harvest failed for $url")
