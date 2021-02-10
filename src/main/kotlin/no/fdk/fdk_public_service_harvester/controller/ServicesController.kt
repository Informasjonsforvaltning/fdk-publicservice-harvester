package no.fdk.fdk_public_service_harvester.controller

import no.fdk.fdk_public_service_harvester.rdf.JenaType
import no.fdk.fdk_public_service_harvester.rdf.jenaTypeFromAcceptHeader
import no.fdk.fdk_public_service_harvester.service.PublicServicesService
import org.slf4j.LoggerFactory
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.stereotype.Controller
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.RequestMapping
import javax.servlet.http.HttpServletRequest

private val LOGGER = LoggerFactory.getLogger(ServicesController::class.java)

@Controller
@RequestMapping(value = ["/public-services"], produces = ["text/turtle", "text/n3", "application/rdf+json", "application/ld+json", "application/rdf+xml", "application/n-triples"])
open class ServicesController(private val publicServicesService: PublicServicesService) {

    @GetMapping("/{id}")
    fun getServiceById(httpServletRequest: HttpServletRequest, @PathVariable id: String): ResponseEntity<String> {
        LOGGER.info("get service with id $id")
        val returnType = jenaTypeFromAcceptHeader(httpServletRequest.getHeader("Accept"))

        return if (returnType == JenaType.NOT_JENA) ResponseEntity(HttpStatus.NOT_ACCEPTABLE)
        else {
            publicServicesService.getServiceById(id, returnType ?: JenaType.TURTLE)
                ?.let { ResponseEntity(it, HttpStatus.OK) }
                ?: ResponseEntity(HttpStatus.NOT_FOUND)
        }
    }

    @GetMapping
    fun getServices(httpServletRequest: HttpServletRequest): ResponseEntity<String> {
        LOGGER.info("get all services")
        val returnType = jenaTypeFromAcceptHeader(httpServletRequest.getHeader("Accept"))

        return if (returnType == JenaType.NOT_JENA) ResponseEntity(HttpStatus.NOT_ACCEPTABLE)
        else ResponseEntity(publicServicesService.getAll(returnType ?: JenaType.TURTLE), HttpStatus.OK)
    }

}
