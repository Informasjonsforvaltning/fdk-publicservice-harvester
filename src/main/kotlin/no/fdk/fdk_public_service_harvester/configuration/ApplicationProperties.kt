package no.fdk.fdk_public_service_harvester.configuration

import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.boot.context.properties.ConstructorBinding

@ConstructorBinding
@ConfigurationProperties("application")
data class ApplicationProperties(
    val publicServiceHarvesterUri: String,
    val harvestAdminRootUrl: String,
    val tmpHarvest: String
)
