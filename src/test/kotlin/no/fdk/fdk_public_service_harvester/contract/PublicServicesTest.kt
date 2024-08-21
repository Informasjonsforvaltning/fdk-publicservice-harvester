package no.fdk.fdk_public_service_harvester.contract

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import no.fdk.fdk_public_service_harvester.model.DuplicateIRI
import no.fdk.fdk_public_service_harvester.utils.*
import no.fdk.fdk_public_service_harvester.utils.jwk.Access
import no.fdk.fdk_public_service_harvester.utils.jwk.JwtToken
import org.apache.jena.riot.Lang
import org.junit.jupiter.api.*
import org.junit.jupiter.api.Assertions.assertEquals
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.web.server.LocalServerPort
import org.springframework.http.HttpMethod
import org.springframework.http.HttpStatus
import org.springframework.test.context.ContextConfiguration
import kotlin.test.assertTrue

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@SpringBootTest(
    properties = ["spring.profiles.active=contract-test"],
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT
)
@ContextConfiguration(initializers = [ApiTestContext.Initializer::class])
@Tag("contract")
class PublicServicesTest : ApiTestContext() {

    @LocalServerPort
    var port: Int = 0

    private val responseReader = TestResponseReader()
    private val mapper = jacksonObjectMapper()

    @Test
    fun findAllWithRecords() {
        val response = apiGet("/public-services?catalogrecords=true", "text/turtle", port)
        Assumptions.assumeTrue(HttpStatus.OK.value() == response["status"])

        val expected = responseReader.parseFile("all_catalogs.ttl", "TURTLE")
        val responseModel = responseReader.parseResponse(response["body"] as String, "TURTLE")

        assertTrue(
            checkIfIsomorphicAndPrintDiff(
                actual = responseModel,
                expected = expected,
                name = "ServicesTest.findAll"
            )
        )
    }

    @Test
    fun findAllNoRecords() {
        val response = apiGet("/public-services", "application/trig", port)
        Assumptions.assumeTrue(HttpStatus.OK.value() == response["status"])

        val expected = responseReader.parseFile("no_meta_all_services.ttl", "TURTLE")
        val responseModel = responseReader.parseResponse(response["body"] as String, Lang.TRIG.name)

        assertTrue(
            checkIfIsomorphicAndPrintDiff(
                actual = responseModel,
                expected = expected,
                name = "ServicesTest.findAll"
            )
        )
    }

    @Test
    fun findSpecificWithRecords() {
        val response = apiGet("/public-services/$SERVICE_ID_0?catalogrecords=true", "application/rdf+json", port)
        Assumptions.assumeTrue(HttpStatus.OK.value() == response["status"])

        val expected = responseReader.parseFile("service_0.ttl", "TURTLE")
        val responseModel = responseReader.parseResponse(response["body"] as String, "RDF/JSON")

        assertTrue(
            checkIfIsomorphicAndPrintDiff(
                actual = responseModel,
                expected = expected,
                name = "ServicesTest.findSpecific"
            )
        )
    }

    @Test
    fun findSpecificNoRecords() {
        val response = apiGet("/public-services/$SERVICE_ID_0", "application/n-quads", port)
        Assumptions.assumeTrue(HttpStatus.OK.value() == response["status"])

        val expected = responseReader.parseFile("no_meta_service_0.ttl", "TURTLE")
        val responseModel = responseReader.parseResponse(response["body"] as String, Lang.NQUADS.name)

        assertTrue(
            checkIfIsomorphicAndPrintDiff(
                actual = responseModel,
                expected = expected,
                name = "ServicesTest.findSpecific"
            )
        )
    }

    @Test
    fun idDoesNotExist() {
        val response = apiGet("/public-services/123", "text/turtle", port)
        Assertions.assertEquals(HttpStatus.NOT_FOUND.value(), response["status"])
    }

    @Nested
    internal inner class RemoveServiceById {

        @Test
        fun unauthorizedForNoToken() {
            val response = authorizedRequest("/public-services/$SERVICE_ID_0/remove", null, port, HttpMethod.POST)
            assertEquals(HttpStatus.UNAUTHORIZED.value(), response["status"])
        }

        @Test
        fun forbiddenWithNonSysAdminRole() {
            val response = authorizedRequest(
                "/public-services/$SERVICE_ID_0/remove",
                JwtToken(Access.ORG_WRITE).toString(),
                port,
                HttpMethod.POST
            )
            assertEquals(HttpStatus.FORBIDDEN.value(), response["status"])
        }

        @Test
        fun notFoundWhenIdNotInDB() {
            val response =
                authorizedRequest("/public-services/123/remove", JwtToken(Access.ROOT).toString(), port, HttpMethod.POST)
            assertEquals(HttpStatus.NOT_FOUND.value(), response["status"])
        }

        @Test
        fun okWithSysAdminRole() {
            val response = authorizedRequest(
                "/public-services/$SERVICE_ID_0/remove",
                JwtToken(Access.ROOT).toString(),
                port,
                HttpMethod.POST
            )
            assertEquals(HttpStatus.OK.value(), response["status"])
        }
    }

    @Nested
    internal inner class RemoveDuplicates {

        @Test
        fun unauthorizedForNoToken() {
            val body = listOf(DuplicateIRI(iriToRemove = SERVICE_META_0.uri, iriToRetain = SERVICE_META_1.uri))
            val response = authorizedRequest(
                "/public-services/remove-duplicates",
                null,
                port,
                HttpMethod.POST,
                mapper.writeValueAsString(body)
            )
            assertEquals(HttpStatus.UNAUTHORIZED.value(), response["status"])
        }

        @Test
        fun forbiddenWithNonSysAdminRole() {
            val body = listOf(DuplicateIRI(iriToRemove = SERVICE_META_0.uri, iriToRetain = SERVICE_META_1.uri))
            val response = authorizedRequest(
                "/public-services/remove-duplicates",
                JwtToken(Access.ORG_WRITE).toString(),
                port,
                HttpMethod.POST,
                mapper.writeValueAsString(body)
            )
            assertEquals(HttpStatus.FORBIDDEN.value(), response["status"])
        }

        @Test
        fun badRequestWhenRemoveIRINotInDB() {
            val body = listOf(DuplicateIRI(iriToRemove = "https://123.no", iriToRetain = SERVICE_META_1.uri))
            val response =
                authorizedRequest(
                    "/public-services/remove-duplicates",
                    JwtToken(Access.ROOT).toString(),
                    port,
                    HttpMethod.POST,
                    mapper.writeValueAsString(body)
                )
            assertEquals(HttpStatus.BAD_REQUEST.value(), response["status"])
        }

        @Test
        fun okWithSysAdminRole() {
            val body = listOf(DuplicateIRI(iriToRemove = SERVICE_META_0.uri, iriToRetain = SERVICE_META_1.uri))
            val response = authorizedRequest(
                "/public-services/remove-duplicates",
                JwtToken(Access.ROOT).toString(),
                port,
                HttpMethod.POST,
                mapper.writeValueAsString(body)
            )
            assertEquals(HttpStatus.OK.value(), response["status"])
        }
    }

    @Nested
    internal inner class PurgeById {

        @Test
        fun unauthorizedForNoToken() {
            val response = authorizedRequest("/public-services/removed", null, port, HttpMethod.DELETE)
            assertEquals(HttpStatus.UNAUTHORIZED.value(), response["status"])
        }

        @Test
        fun forbiddenWithNonSysAdminRole() {
            val response = authorizedRequest(
                "/public-services/removed",
                JwtToken(Access.ORG_WRITE).toString(),
                port,
                HttpMethod.DELETE
            )
            assertEquals(HttpStatus.FORBIDDEN.value(), response["status"])
        }

        @Test
        fun badRequestWhenNotAlreadyRemoved() {
            val response = authorizedRequest(
                "/public-services/$SERVICE_ID_1",
                JwtToken(Access.ROOT).toString(),
                port,
                HttpMethod.DELETE
            )
            assertEquals(HttpStatus.BAD_REQUEST.value(), response["status"])
        }

        @Test
        fun purgingStopsDeepLinking() {
            val pre = apiGet("/public-services/removed", "text/turtle", port)
            assertEquals(HttpStatus.OK.value(), pre["status"])

            val response = authorizedRequest(
                "/public-services/removed",
                JwtToken(Access.ROOT).toString(),
                port,
                HttpMethod.DELETE
            )
            assertEquals(HttpStatus.NO_CONTENT.value(), response["status"])

            val post = apiGet("/public-services/removed", "text/turtle", port)
            assertEquals(HttpStatus.NOT_FOUND.value(), post["status"])
        }

    }

}
