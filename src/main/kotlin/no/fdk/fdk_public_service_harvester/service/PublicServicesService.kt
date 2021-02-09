package no.fdk.fdk_public_service_harvester.service

import no.fdk.fdk_public_service_harvester.rdf.JenaType
import no.fdk.fdk_public_service_harvester.rdf.createRDFResponse
import no.fdk.fdk_public_service_harvester.rdf.parseRDFResponse
import org.apache.jena.rdf.model.ModelFactory
import org.springframework.stereotype.Service

@Service
class PublicServicesService(private val turtleService: TurtleService) {

    fun getAll(returnType: JenaType): String =
        turtleService.getUnion()
            ?.let {
                if (returnType == JenaType.TURTLE) it
                else parseRDFResponse(it, JenaType.TURTLE, null)?.createRDFResponse(returnType)
            }
            ?: ModelFactory.createDefaultModel().createRDFResponse(returnType)

    fun getServiceById(id: String, returnType: JenaType): String? =
        turtleService.getPublicService(id, withRecords = true)
            ?.let {
                if (returnType == JenaType.TURTLE) it
                else parseRDFResponse(it, JenaType.TURTLE, null)?.createRDFResponse(returnType)
            }

}
