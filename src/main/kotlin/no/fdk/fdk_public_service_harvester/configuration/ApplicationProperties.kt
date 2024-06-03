package no.fdk.fdk_public_service_harvester.configuration

import org.springframework.boot.context.properties.ConfigurationProperties

@ConfigurationProperties("application")
data class ApplicationProperties(
    val organizationsUri: String,
    val publicServiceHarvesterUri: String,
    val harvestAdminRootUrl: String,
    val harvestAdminApiKey: String
)
