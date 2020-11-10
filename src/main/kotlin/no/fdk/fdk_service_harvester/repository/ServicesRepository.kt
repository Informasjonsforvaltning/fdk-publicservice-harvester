package no.fdk.fdk_service_harvester.repository

import no.fdk.fdk_service_harvester.model.ServiceDBO
import org.springframework.data.mongodb.repository.MongoRepository
import org.springframework.stereotype.Repository

@Repository
interface ServicesRepository : MongoRepository<ServiceDBO, String> {
    fun findOneByFdkId(fdkId: String): ServiceDBO?
}