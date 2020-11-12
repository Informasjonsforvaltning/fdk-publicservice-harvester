package no.fdk.fdk_service_harvester.configuration

import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.boot.context.properties.ConstructorBinding

@ConstructorBinding
@ConfigurationProperties("application")
data class ApplicationProperties(
    val serviceHarvesterUri: String,
    val harvestAdminRootUrl: String,
    val tmpHarvest: String
)
