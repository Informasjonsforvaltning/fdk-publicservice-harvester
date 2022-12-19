package no.fdk.fdk_public_service_harvester.utils

import no.fdk.fdk_public_service_harvester.model.*
import no.fdk.fdk_public_service_harvester.service.UNION_ID
import no.fdk.fdk_public_service_harvester.service.gzip
import org.bson.Document

private val responseReader = TestResponseReader()


val SERVICE_META_0 = PublicServiceMeta(
    uri = "http://public-service-publisher.fellesdatakatalog.digdir.no/services/1",
    fdkId = SERVICE_ID_0,
    issued = TEST_HARVEST_DATE.timeInMillis,
    modified = TEST_HARVEST_DATE.timeInMillis
)

val SERVICE_TURTLE_0_META = FDKPublicServiceTurtle(
    id = SERVICE_ID_0,
    turtle = gzip(responseReader.readFile("service_0.ttl"))
)

val SERVICE_TURTLE_0_NO_META = PublicServiceTurtle(
    id = SERVICE_ID_0,
    turtle = gzip(responseReader.readFile("no_meta_service_0.ttl"))
)

val SERVICE_META_1 = PublicServiceMeta(
    uri = "http://public-service-publisher.fellesdatakatalog.digdir.no/services/2",
    fdkId = SERVICE_ID_1,
    issued = TEST_HARVEST_DATE.timeInMillis,
    modified = TEST_HARVEST_DATE.timeInMillis
)

val SERVICE_TURTLE_1_META = FDKPublicServiceTurtle(
    id = SERVICE_ID_1,
    turtle = gzip(responseReader.readFile("service_1.ttl"))
)

val SERVICE_TURTLE_1_NO_META = PublicServiceTurtle(
    id = SERVICE_ID_1,
    turtle = gzip(responseReader.readFile("no_meta_service_1.ttl"))
)

val SERVICE_META_2 = PublicServiceMeta(
    uri = "http://public-service-publisher.fellesdatakatalog.digdir.no/services/3",
    fdkId = SERVICE_ID_2,
    issued = TEST_HARVEST_DATE.timeInMillis,
    modified = TEST_HARVEST_DATE.timeInMillis
)

val SERVICE_TURTLE_2_META = FDKPublicServiceTurtle(
    id = SERVICE_ID_2,
    turtle = gzip(responseReader.readFile("service_2.ttl"))
)

val SERVICE_TURTLE_2_NO_META = PublicServiceTurtle(
    id = SERVICE_ID_2,
    turtle = gzip(responseReader.readFile("no_meta_service_2.ttl"))
)

val SERVICE_META_3 = PublicServiceMeta(
    uri = "https://raw.githubusercontent.com/Informasjonsforvaltning/cpsv-ap-no/develop/examples/exTjenesteDummy.ttl",
    fdkId = SERVICE_ID_3,
    issued = TEST_HARVEST_DATE.timeInMillis,
    modified = TEST_HARVEST_DATE.timeInMillis
)

val SERVICE_META_4 = PublicServiceMeta(
    uri = "http://test.no/services/0",
    fdkId = "ef4ca382-ee65-3a92-be9e-40fd93da53bc",
    isPartOf = "http://localhost:5000/public-services/catalogs/$CATALOG_ID_1",
    issued = TEST_HARVEST_DATE.timeInMillis,
    modified = TEST_HARVEST_DATE.timeInMillis
)

val SERVICE_META_5 = PublicServiceMeta(
    uri = "http://test.no/services/1",
    fdkId = "7baa248b-1a27-3c46-80cf-889882d6b894",
    isPartOf = "http://localhost:5000/public-services/catalogs/$CATALOG_ID_1",
    issued = TEST_HARVEST_DATE.timeInMillis,
    modified = TEST_HARVEST_DATE.timeInMillis
)

val CATALOG_META_1 = CatalogMeta(
    uri = "http://test.no/catalogs/0",
    fdkId = CATALOG_ID_1,
    issued = TEST_HARVEST_DATE.timeInMillis,
    modified = TEST_HARVEST_DATE.timeInMillis,
    services = setOf("http://test.no/services/0", "http://test.no/services/1")
)

val SERVICE_TURTLE_3_META = FDKPublicServiceTurtle(
    id = SERVICE_ID_3,
    turtle = gzip(responseReader.readFile("service_3.ttl"))
)

val SERVICE_TURTLE_3_NO_META = PublicServiceTurtle(
    id = SERVICE_ID_3,
    turtle = gzip(responseReader.readFile("no_meta_service_3.ttl"))
)

val SERVICE_TURTLE_4_META = FDKPublicServiceTurtle(
    id = SERVICE_ID_4,
    turtle = gzip(responseReader.readFile("service_4.ttl"))
)

val SERVICE_TURTLE_4_NO_META = PublicServiceTurtle(
    id = SERVICE_ID_4,
    turtle = gzip(responseReader.readFile("no_meta_service_4.ttl"))
)

val SERVICE_TURTLE_5_META = FDKPublicServiceTurtle(
    id = SERVICE_ID_5,
    turtle = gzip(responseReader.readFile("service_5.ttl"))
)

val SERVICE_TURTLE_5_NO_META = PublicServiceTurtle(
    id = SERVICE_ID_5,
    turtle = gzip(responseReader.readFile("no_meta_service_5.ttl"))
)

val HARVESTED_DBO = HarvestSourceTurtle(
    id = "http://localhost:5000/fdk-public-service-publisher.ttl",
    turtle = gzip(responseReader.readFile("harvest_response_0.ttl"))
)

val UNION_DATA = FDKPublicServiceTurtle(
    id = UNION_ID,
    turtle = gzip(responseReader.readFile("all_services.ttl"))
)

val UNION_DATA_NO_META = PublicServiceTurtle(
    id = UNION_ID,
    turtle = gzip(responseReader.readFile("no_meta_all_services.ttl"))
)

val CATALOG_UNION_DATA = FDKPublicServiceTurtle(
    id = UNION_ID,
    turtle = gzip(responseReader.readFile("catalog_1.ttl"))
)

val CATALOG_UNION_DATA_NO_META = FDKPublicServiceTurtle(
    id = UNION_ID,
    turtle = gzip(responseReader.readFile("harvest_response_1.ttl"))
)

val CATALOG_1 = FDKPublicServiceTurtle(
    id = CATALOG_ID_1,
    turtle = gzip(responseReader.readFile("catalog_1.ttl"))
)

val CATALOG_1_NO_META = FDKPublicServiceTurtle(
    id = CATALOG_ID_1,
    turtle = gzip(responseReader.readFile("harvest_response_1.ttl"))
)

fun serviceTurtleDBPopulation(): List<Document> =
    listOf(SERVICE_TURTLE_0_NO_META, SERVICE_TURTLE_1_NO_META, SERVICE_TURTLE_2_NO_META,
        UNION_DATA_NO_META, SERVICE_TURTLE_3_NO_META, SERVICE_TURTLE_4_NO_META,
        SERVICE_TURTLE_5_NO_META)
        .map { it.mapDBO() }

fun fdkTurtleDBPopulation(): List<Document> =
    listOf(UNION_DATA, SERVICE_TURTLE_0_META, SERVICE_TURTLE_1_META,
        SERVICE_TURTLE_2_META, SERVICE_TURTLE_3_META, SERVICE_TURTLE_4_META,
        SERVICE_TURTLE_5_META)
        .map { it.mapDBO() }

fun sourceTurtleDBPopulation(): List<Document> =
    listOf(HARVESTED_DBO).map { it.mapDBO() }

fun metaDBPopulation(): List<Document> =
    listOf(SERVICE_META_0, SERVICE_META_1, SERVICE_META_2, SERVICE_META_3, SERVICE_META_4, SERVICE_META_5)
        .map { it.mapDBO() }

fun metaCatalogPopulation(): List<Document> =
    listOf(CATALOG_META_1)
        .map { it.mapDBO() }

fun fdkCatalogTurtleDBPopulation(): List<Document> =
    listOf(CATALOG_1, CATALOG_UNION_DATA).map { it.mapDBO() }

fun catalogTurtleDBPopulation(): List<Document> =
    listOf(CATALOG_1_NO_META, CATALOG_UNION_DATA_NO_META).map { it.mapDBO() }

private fun PublicServiceMeta.mapDBO(): Document =
    Document()
        .append("_id", uri)
        .append("fdkId", fdkId)
        .append("isPartOf", isPartOf)
        .append("issued", issued)
        .append("modified", modified)

private fun CatalogMeta.mapDBO(): Document =
    Document()
        .append("_id", uri)
        .append("fdkId", fdkId)
        .append("issued", issued)
        .append("modified", modified)
        .append("services", services)

private fun TurtleDBO.mapDBO(): Document =
    Document()
        .append("_id", id)
        .append("turtle", turtle)
