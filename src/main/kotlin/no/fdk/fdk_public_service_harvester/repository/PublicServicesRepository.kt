package no.fdk.fdk_public_service_harvester.repository

import no.fdk.fdk_public_service_harvester.model.PublicServiceDBO
import org.springframework.data.mongodb.repository.MongoRepository
import org.springframework.stereotype.Repository

@Repository
interface PublicServicesRepository : MongoRepository<PublicServiceDBO, String> {
    fun findOneByFdkId(fdkId: String): PublicServiceDBO?
}