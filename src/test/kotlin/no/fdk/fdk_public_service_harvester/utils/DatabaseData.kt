package no.fdk.fdk_public_service_harvester.utils

import no.fdk.fdk_public_service_harvester.model.*
import no.fdk.fdk_public_service_harvester.service.UNION_ID
import no.fdk.fdk_public_service_harvester.service.gzip
import no.fdk.fdk_public_service_harvester.service.turtleId
import org.bson.Document

private val responseReader = TestResponseReader()


val SERVICE_META_0 = PublicServiceMeta(
    uri = "http://public-service-publisher.fellesdatakatalog.digdir.no/services/1",
    fdkId = SERVICE_ID_0,
    issued = TEST_HARVEST_DATE.timeInMillis,
    modified = TEST_HARVEST_DATE.timeInMillis
)

val SERVICE_TURTLE_0_META = TurtleDBO(
    id = turtleId(SERVICE_ID_0, withRecords = true),
    turtle = gzip(responseReader.readFile("service_0.ttl"))
)

val SERVICE_TURTLE_0_NO_META = TurtleDBO(
    id = turtleId(SERVICE_ID_0, withRecords = false),
    turtle = gzip(responseReader.readFile("no_meta_service_0.ttl"))
)

val SERVICE_META_1 = PublicServiceMeta(
    uri = "http://public-service-publisher.fellesdatakatalog.digdir.no/services/2",
    fdkId = SERVICE_ID_1,
    issued = TEST_HARVEST_DATE.timeInMillis,
    modified = TEST_HARVEST_DATE.timeInMillis
)

val SERVICE_TURTLE_1_META = TurtleDBO(
    id = turtleId(SERVICE_ID_1, withRecords = true),
    turtle = gzip(responseReader.readFile("service_1.ttl"))
)

val SERVICE_TURTLE_1_NO_META = TurtleDBO(
    id = turtleId(SERVICE_ID_1, withRecords = false),
    turtle = gzip(responseReader.readFile("no_meta_service_1.ttl"))
)

val SERVICE_META_2 = PublicServiceMeta(
    uri = "http://public-service-publisher.fellesdatakatalog.digdir.no/services/3",
    fdkId = SERVICE_ID_2,
    issued = TEST_HARVEST_DATE.timeInMillis,
    modified = TEST_HARVEST_DATE.timeInMillis
)

val SERVICE_TURTLE_2_META = TurtleDBO(
    id = turtleId(SERVICE_ID_2, withRecords = true),
    turtle = gzip(responseReader.readFile("service_2.ttl"))
)

val SERVICE_TURTLE_2_NO_META = TurtleDBO(
    id = turtleId(SERVICE_ID_2, withRecords = false),
    turtle = gzip(responseReader.readFile("no_meta_service_2.ttl"))
)

val SERVICE_META_3 = PublicServiceMeta(
    uri = "https://raw.githubusercontent.com/Informasjonsforvaltning/cpsv-ap-no/develop/examples/exTjenesteDummy.ttl",
    fdkId = SERVICE_ID_3,
    issued = TEST_HARVEST_DATE.timeInMillis,
    modified = TEST_HARVEST_DATE.timeInMillis
)

val SERVICE_TURTLE_3_META = TurtleDBO(
    id = turtleId(SERVICE_ID_3, withRecords = true),
    turtle = gzip(responseReader.readFile("service_3.ttl"))
)

val SERVICE_TURTLE_3_NO_META = TurtleDBO(
    id = turtleId(SERVICE_ID_3, withRecords = false),
    turtle = gzip(responseReader.readFile("no_meta_service_3.ttl"))
)

val HARVESTED_DBO = TurtleDBO(
    id = "http://localhost:5000/fdk-public-service-publisher.ttl",
    turtle = gzip(responseReader.readFile("harvest_response_0.ttl"))
)

val UNION_DATA = TurtleDBO(
    id = turtleId(UNION_ID, withRecords = true),
    turtle = gzip(responseReader.readFile("all_services.ttl"))
)

val UNION_DATA_NO_META = TurtleDBO(
    id = turtleId(UNION_ID, withRecords = false),
    turtle = gzip(responseReader.readFile("no_meta_all_services.ttl"))
)

fun turleDBPopulation(): List<Document> =
    listOf(UNION_DATA, HARVESTED_DBO, SERVICE_TURTLE_0_META, SERVICE_TURTLE_0_NO_META, SERVICE_TURTLE_1_META,
        SERVICE_TURTLE_1_NO_META, SERVICE_TURTLE_2_META, SERVICE_TURTLE_2_NO_META, UNION_DATA_NO_META,
        SERVICE_TURTLE_3_NO_META, SERVICE_TURTLE_3_META)
        .map { it.mapDBO() }

fun metaDBPopulation(): List<Document> =
    listOf(SERVICE_META_0, SERVICE_META_1, SERVICE_META_2, SERVICE_META_3)
        .map { it.mapDBO() }

private fun PublicServiceMeta.mapDBO(): Document =
    Document()
        .append("_id", uri)
        .append("fdkId", fdkId)
        .append("issued", issued)
        .append("modified", modified)

private fun TurtleDBO.mapDBO(): Document =
    Document()
        .append("_id", id)
        .append("turtle", turtle)
