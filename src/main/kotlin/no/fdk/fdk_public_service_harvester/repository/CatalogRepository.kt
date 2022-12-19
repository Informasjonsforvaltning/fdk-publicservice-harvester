package no.fdk.fdk_public_service_harvester.repository

import no.fdk.fdk_public_service_harvester.model.CatalogMeta
import org.springframework.data.mongodb.repository.MongoRepository
import org.springframework.stereotype.Repository

@Repository
interface CatalogRepository : MongoRepository<CatalogMeta, String>
