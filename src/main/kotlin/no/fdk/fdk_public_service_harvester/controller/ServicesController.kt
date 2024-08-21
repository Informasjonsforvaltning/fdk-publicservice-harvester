package no.fdk.fdk_public_service_harvester.controller

import jakarta.servlet.http.HttpServletRequest
import no.fdk.fdk_public_service_harvester.model.DuplicateIRI
import no.fdk.fdk_public_service_harvester.rdf.jenaTypeFromAcceptHeader
import no.fdk.fdk_public_service_harvester.service.EndpointPermissions
import no.fdk.fdk_public_service_harvester.service.PublicServicesService
import org.apache.jena.riot.Lang
import org.slf4j.LoggerFactory
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.security.core.annotation.AuthenticationPrincipal
import org.springframework.security.oauth2.jwt.Jwt
import org.springframework.stereotype.Controller
import org.springframework.web.bind.annotation.DeleteMapping
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestParam

private val LOGGER = LoggerFactory.getLogger(ServicesController::class.java)

@Controller
@RequestMapping(
    value = ["/public-services"],
    produces = ["text/turtle", "text/n3", "application/rdf+json", "application/ld+json", "application/rdf+xml",
        "application/n-triples", "application/n-quads", "application/trig", "application/trix"]
)
open class ServicesController(
    private val publicServicesService: PublicServicesService,
    private val endpointPermissions: EndpointPermissions
) {

    @GetMapping("/{id}")
    fun getServiceById(
        httpServletRequest: HttpServletRequest,
        @PathVariable id: String,
        @RequestParam(value = "catalogrecords", required = false) catalogRecords: Boolean = false
    ): ResponseEntity<String> {
        LOGGER.info("get service with id $id")
        val returnType = jenaTypeFromAcceptHeader(httpServletRequest.getHeader("Accept"))

        return if (returnType == Lang.RDFNULL) ResponseEntity(HttpStatus.NOT_ACCEPTABLE)
        else {
            publicServicesService.getServiceById(id, returnType ?: Lang.TURTLE, catalogRecords)
                ?.let { ResponseEntity(it, HttpStatus.OK) }
                ?: ResponseEntity(HttpStatus.NOT_FOUND)
        }
    }

    @GetMapping
    fun getServices(
        httpServletRequest: HttpServletRequest,
        @RequestParam(value = "catalogrecords", required = false) catalogRecords: Boolean = false
    ): ResponseEntity<String> {
        LOGGER.info("get all services")
        val returnType = jenaTypeFromAcceptHeader(httpServletRequest.getHeader("Accept"))

        return if (returnType == Lang.RDFNULL) ResponseEntity(HttpStatus.NOT_ACCEPTABLE)
        else ResponseEntity(publicServicesService.getAllServices(returnType ?: Lang.TURTLE, catalogRecords), HttpStatus.OK)
    }

    @PostMapping("/{id}/remove")
    fun removeServiceById(
        @AuthenticationPrincipal jwt: Jwt,
        @PathVariable id: String
    ): ResponseEntity<Void> =
        if (endpointPermissions.hasAdminPermission(jwt)) {
            publicServicesService.removeService(id)
            ResponseEntity(HttpStatus.OK)
        } else ResponseEntity(HttpStatus.FORBIDDEN)

    @PostMapping("/duplicates")
    fun removeDuplicates(
        @AuthenticationPrincipal jwt: Jwt,
        @RequestBody duplicates: List<DuplicateIRI>
    ): ResponseEntity<Void> =
        if (endpointPermissions.hasAdminPermission(jwt)) {
            publicServicesService.removeDuplicates(duplicates)
            ResponseEntity(HttpStatus.OK)
        } else ResponseEntity(HttpStatus.FORBIDDEN)

}
