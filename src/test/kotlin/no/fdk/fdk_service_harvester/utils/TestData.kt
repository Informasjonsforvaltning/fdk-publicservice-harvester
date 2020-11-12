package no.fdk.fdk_service_harvester.utils

import no.fdk.fdk_service_harvester.model.HarvestDataSource
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap
import java.util.*

const val LOCAL_SERVER_PORT = 5000

const val MONGO_USER = "testuser"
const val MONGO_PASSWORD = "testpassword"
const val MONGO_PORT = 27017
const val MONGO_COLLECTION = "serviceHarvester"

val MONGO_ENV_VALUES: Map<String, String> = ImmutableMap.of(
    "MONGO_INITDB_ROOT_USERNAME", MONGO_USER,
    "MONGO_INITDB_ROOT_PASSWORD", MONGO_PASSWORD
)

const val SERVICE_ID_0 = "d5d0c07c-c14f-3741-9aa3-126960958cf0"
const val SERVICE_ID_1 = "6ce4e524-3226-3591-ad99-c026705d4259"
const val SERVICE_ID_2 = "31249174-df02-3746-9d61-59fc61b4c5f9"

val TEST_HARVEST_DATE: Calendar = Calendar.Builder().setTimeZone(TimeZone.getTimeZone("UTC")).setDate(2020, 9, 5).setTimeOfDay(13, 15, 39, 831).build()
val NEW_TEST_HARVEST_DATE: Calendar = Calendar.Builder().setTimeZone(TimeZone.getTimeZone("UTC")).setDate(2020, 9, 15).setTimeOfDay(11, 52, 16, 122).build()

val TEST_HARVEST_SOURCE = HarvestDataSource(
    url = "http://localhost:5000/fdk-public-service-publisher.ttl",
    acceptHeaderValue = "text/turtle",
    dataType = "service",
    dataSourceType = "DCAT-AP-NO"
)
