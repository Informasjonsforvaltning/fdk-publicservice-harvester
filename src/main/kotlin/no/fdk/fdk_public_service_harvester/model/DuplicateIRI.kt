package no.fdk.fdk_public_service_harvester.model

data class DuplicateIRI (
    val iriToRetain: String,
    val iriToRemove: String,
    val keepRemovedFdkId: Boolean = true
)
