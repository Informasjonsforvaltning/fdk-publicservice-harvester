package no.fdk.fdk_publicservice_harvester.service

import no.fdk.fdk_publicservice_harvester.model.UNION_ID
import no.fdk.fdk_publicservice_harvester.repository.PublicServicesRepository
import no.fdk.fdk_publicservice_harvester.repository.MiscellaneousRepository
import no.fdk.fdk_publicservice_harvester.rdf.JenaType
import no.fdk.fdk_publicservice_harvester.rdf.createRDFResponse
import no.fdk.fdk_publicservice_harvester.rdf.parseRDFResponse
import org.apache.jena.rdf.model.ModelFactory
import org.springframework.data.repository.findByIdOrNull
import org.springframework.stereotype.Service

@Service
class PublicServicesService(
    private val publicServicesRepository: PublicServicesRepository,
    private val miscellaneousRepository: MiscellaneousRepository
) {

    fun getAll(returnType: JenaType): String =
        miscellaneousRepository.findByIdOrNull(UNION_ID)
            ?.let { ungzip(it.turtle) }
            ?.let {
                if (returnType == JenaType.TURTLE) it
                else parseRDFResponse(it, JenaType.TURTLE, null)?.createRDFResponse(returnType)
            }
            ?: ModelFactory.createDefaultModel().createRDFResponse(returnType)

    fun getServiceById(id: String, returnType: JenaType): String? =
        publicServicesRepository.findOneByFdkId(id)
            ?.let { ungzip(it.turtleService) }
            ?.let {
                if (returnType == JenaType.TURTLE) it
                else parseRDFResponse(it, JenaType.TURTLE, null)?.createRDFResponse(returnType)
            }

}
