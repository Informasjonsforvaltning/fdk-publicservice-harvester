package no.fdk.fdk_public_service_harvester.harvester

import no.fdk.fdk_public_service_harvester.Application
import no.fdk.fdk_public_service_harvester.model.Organization
import no.fdk.fdk_public_service_harvester.rdf.*
import org.apache.jena.query.QueryExecutionFactory
import org.apache.jena.query.QueryFactory
import org.apache.jena.rdf.model.*
import org.apache.jena.riot.Lang
import org.apache.jena.util.ResourceUtils
import org.apache.jena.vocabulary.*
import org.slf4j.LoggerFactory
import java.util.*

private val LOGGER = LoggerFactory.getLogger(Application::class.java)

fun CatalogRDFModel.harvestDiff(dbTurtle: String?): Boolean =
    if (dbTurtle == null) true
    else !harvested.isIsomorphicWith(safeParseRDF(dbTurtle, Lang.TURTLE))

fun PublicServiceRDFModel.harvestDiff(dbTurtle: String?): Boolean =
    if (dbTurtle == null) true
    else !harvested.isIsomorphicWith(safeParseRDF(dbTurtle, Lang.TURTLE))

fun splitCatalogsFromRDF(harvested: Model, allServices: List<PublicServiceRDFModel>,
                         sourceURL: String, organization: Organization?): List<CatalogRDFModel> {
    val harvestedCatalogs = harvested.listResourcesWithProperty(RDF.type, DCAT.Catalog)
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
                .recursiveBlankNodeSkolem(resource.uri)

            val catalogModel = ModelFactory.createDefaultModel()
            allServices.filter { catalogServices.contains(it.resourceURI) }
                .forEach { catalogModel.add(it.harvested) }

            CatalogRDFModel(
                resourceURI = resource.uri,
                harvestedWithoutServices = catalogModelWithoutServices,
                harvested = catalogModel.union(catalogModelWithoutServices),
                services = catalogServices
            )
        }

    return harvestedCatalogs.plus(generatedCatalog(
        allServices.filterNot { it.isMemberOfAnyCatalog },
        sourceURL,
        organization)
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
    val serviceModel = listProperties().toModel()
    serviceModel.setNsPrefixes(model.nsPrefixMap)

    listProperties().toList()
        .filter { it.isResourceProperty() }
        .forEach { serviceModel.recursiveAddNonPublicServiceResources(it.resource, 10) }

    return PublicServiceRDFModel(
        resourceURI = uri,
        harvested = serviceModel.recursiveBlankNodeSkolem(uri),
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

private fun generatedCatalog(
    services: List<PublicServiceRDFModel>,
    sourceURL: String,
    organization: Organization?
): CatalogRDFModel {
    val serviceURIs = services.map { it.resourceURI }.toSet()
    val generatedCatalogURI = "$sourceURL#GeneratedCatalog"
    val catalogModelWithoutServices = createModelForHarvestSourceCatalog(generatedCatalogURI, serviceURIs, organization)

    val catalogModel = ModelFactory.createDefaultModel()
    services.forEach { catalogModel.add(it.harvested) }

    return CatalogRDFModel(
        resourceURI = generatedCatalogURI,
        harvestedWithoutServices = catalogModelWithoutServices,
        harvested = catalogModel.union(catalogModelWithoutServices),
        services = serviceURIs
    )
}

private fun createModelForHarvestSourceCatalog(
    catalogURI: String,
    services: Set<String>,
    organization: Organization?
): Model {
    val catalogModel = ModelFactory.createDefaultModel()
    catalogModel.createResource(catalogURI)
        .addProperty(RDF.type, DCAT.Catalog)
        .addPublisherForGeneratedCatalog(organization?.uri)
        .addLabelForGeneratedCatalog(organization)
        .addServicesForGeneratedCatalog(services)

    return catalogModel
}

private fun Resource.addPublisherForGeneratedCatalog(publisherURI: String?): Resource {
    if (publisherURI != null) {
        addProperty(
            DCTerms.publisher,
            ResourceFactory.createResource(publisherURI)
        )
    }

    return this
}

private fun Resource.addLabelForGeneratedCatalog(organization: Organization?): Resource {
    val nb: String? = organization?.prefLabel?.nb ?: organization?.name
    if (!nb.isNullOrBlank()) {
        val label = model.createLiteral("$nb - Tjenestekatalog", "nb")
        addProperty(RDFS.label, label)
    }

    val nn: String? = organization?.prefLabel?.nn ?: organization?.name
    if (!nb.isNullOrBlank()) {
        val label = model.createLiteral("$nn - Tjenestekatalog", "nn")
        addProperty(RDFS.label, label)
    }

    val en: String? = organization?.prefLabel?.en ?: organization?.name
    if (!en.isNullOrBlank()) {
        val label = model.createLiteral("$en - Service catalog", "en")
        addProperty(RDFS.label, label)
    }

    return this
}

private fun Resource.addServicesForGeneratedCatalog(services: Set<String>): Resource {
    services.forEach { addProperty(DCATNO.containsService, model.createResource(it)) }
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

private fun Model.recursiveBlankNodeSkolem(baseURI: String): Model {
    val anonSubjects = listSubjects().toList().filter { it.isAnon }
    return if (anonSubjects.isEmpty()) this
    else {
        anonSubjects
            .filter { it.doesNotContainAnon() }
            .forEach {
                ResourceUtils.renameResource(it, "$baseURI/.well-known/skolem/${it.createSkolemID()}")
            }
        this.recursiveBlankNodeSkolem(baseURI)
    }
}

private fun Resource.doesNotContainAnon(): Boolean =
    listProperties().toList()
        .filter { it.isResourceProperty() }
        .map { it.resource }
        .filter { it.listProperties().toList().size > 0 }
        .none { it.isAnon }

private fun Resource.createSkolemID(): String =
    createIdFromString(
        listProperties().toModel()
            .createRDFResponse(Lang.N3)
            .replace("\\s".toRegex(), "")
            .toCharArray()
            .sorted()
            .toString()
    )

fun createIdFromString(idBase: String): String =
    UUID.nameUUIDFromBytes(idBase.toByteArray())
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

fun List<PublicServiceRDFModel>.containsFreeServices(): Boolean =
    firstOrNull { !it.isMemberOfAnyCatalog } != null

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
