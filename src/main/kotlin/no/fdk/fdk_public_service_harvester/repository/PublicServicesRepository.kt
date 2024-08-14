package no.fdk.fdk_public_service_harvester.repository

import no.fdk.fdk_public_service_harvester.model.PublicServiceMeta
import org.springframework.data.mongodb.repository.MongoRepository
import org.springframework.stereotype.Repository

@Repository
interface PublicServicesRepository : MongoRepository<PublicServiceMeta, String> {
    fun findAllByIsPartOf(isPartOf: String): List<PublicServiceMeta>
    fun findAllByFdkId(fdkId: String): List<PublicServiceMeta>
}
