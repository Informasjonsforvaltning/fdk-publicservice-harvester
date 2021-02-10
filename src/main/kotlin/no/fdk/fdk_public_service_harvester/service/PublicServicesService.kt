package no.fdk.fdk_public_service_harvester.service

import no.fdk.fdk_public_service_harvester.rdf.createRDFResponse
import no.fdk.fdk_public_service_harvester.rdf.parseRDFResponse
import org.apache.jena.rdf.model.ModelFactory
import org.apache.jena.riot.Lang
import org.springframework.stereotype.Service

@Service
class PublicServicesService(private val turtleService: TurtleService) {

    fun getAll(returnType: Lang): String =
        turtleService.getUnion()
            ?.let {
                if (returnType == Lang.TURTLE) it
                else parseRDFResponse(it, Lang.TURTLE, null)?.createRDFResponse(returnType)
            }
            ?: ModelFactory.createDefaultModel().createRDFResponse(returnType)

    fun getServiceById(id: String, returnType: Lang): String? =
        turtleService.getPublicService(id, withRecords = true)
            ?.let {
                if (returnType == Lang.TURTLE) it
                else parseRDFResponse(it, Lang.TURTLE, null)?.createRDFResponse(returnType)
            }

}
