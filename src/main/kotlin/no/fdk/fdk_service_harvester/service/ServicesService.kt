package no.fdk.fdk_service_harvester.service

import no.fdk.fdk_service_harvester.model.UNION_ID
import no.fdk.fdk_service_harvester.repository.ServicesRepository
import no.fdk.fdk_service_harvester.repository.MiscellaneousRepository
import no.fdk.fdk_service_harvester.rdf.JenaType
import no.fdk.fdk_service_harvester.rdf.createRDFResponse
import no.fdk.fdk_service_harvester.rdf.parseRDFResponse
import org.apache.jena.rdf.model.ModelFactory
import org.springframework.data.repository.findByIdOrNull
import org.springframework.stereotype.Service

@Service
class ServicesService(
    private val servicesRepository: ServicesRepository,
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
        servicesRepository.findOneByFdkId(id)
            ?.let { ungzip(it.turtleService) }
            ?.let {
                if (returnType == JenaType.TURTLE) it
                else parseRDFResponse(it, JenaType.TURTLE, null)?.createRDFResponse(returnType)
            }

}
