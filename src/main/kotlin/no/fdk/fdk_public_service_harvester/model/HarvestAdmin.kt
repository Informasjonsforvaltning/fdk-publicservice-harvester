package no.fdk.fdk_public_service_harvester.model

import com.fasterxml.jackson.annotation.JsonIgnoreProperties

@JsonIgnoreProperties(ignoreUnknown=true)
data class HarvestDataSource (
    val dataSourceType: String? = null,
    val dataType: String? = null,
    val url: String? = null,
    val acceptHeaderValue: String? = null
)

@JsonIgnoreProperties(ignoreUnknown=true)
data class HarvestAdminParameters(
<<<<<<< Updated upstream:src/main/kotlin/no/fdk/fdk_public_service_harvester/model/HarvestAdmin.kt
    val dataSourceId: String? = null,
=======
>>>>>>> Stashed changes:src/main/kotlin/no/fdk/fdk_public_service_harvester/model/HarvestDataSource.kt
    val publisherId: String? = null,
    val dataSourceType: String? = null,
    val dataType: String? = "publicService"
)
