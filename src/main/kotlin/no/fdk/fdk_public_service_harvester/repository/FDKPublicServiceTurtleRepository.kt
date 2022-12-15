package no.fdk.fdk_public_service_harvester.repository

import no.fdk.fdk_public_service_harvester.model.FDKPublicServiceTurtle
import org.springframework.data.mongodb.repository.MongoRepository
import org.springframework.stereotype.Repository

@Repository
interface FDKPublicServiceTurtleRepository : MongoRepository<FDKPublicServiceTurtle, String>
