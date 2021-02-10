package no.fdk.fdk_public_service_harvester.controller

import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RestController

@RestController
class ApplicationStatusController {

    @GetMapping("/ping")
    fun ping(): ResponseEntity<Void> =
        ResponseEntity.ok().build()

    @GetMapping("/ready")
    fun ready(): ResponseEntity<Void> =
        ResponseEntity.ok().build()

}
