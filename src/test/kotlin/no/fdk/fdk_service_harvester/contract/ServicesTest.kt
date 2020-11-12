package no.fdk.fdk_service_harvester.contract

import no.fdk.fdk_service_harvester.utils.*
import org.junit.jupiter.api.*
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.web.server.LocalServerPort
import org.springframework.http.HttpStatus
import org.springframework.test.context.ContextConfiguration
import kotlin.test.assertTrue

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@SpringBootTest(
    properties = ["spring.profiles.active=contract-test"],
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ContextConfiguration(initializers = [ApiTestContext.Initializer::class])
@Tag("contract")
class ServicesTest: ApiTestContext() {

    @LocalServerPort
    var port: Int = 0

    private val responseReader = TestResponseReader()

    @Test
    fun findAll() {
        val response = apiGet("/services", "text/turtle", port)
        Assumptions.assumeTrue(HttpStatus.OK.value() == response["status"])

        val expected = responseReader.parseFile("all_services.ttl", "TURTLE")
        val responseModel = responseReader.parseResponse(response["body"] as String, "TURTLE")

        assertTrue(checkIfIsomorphicAndPrintDiff(actual = responseModel, expected = expected, name = "ServicesTest.findAll"))
    }

    @Test
    fun findSpecific() {
        val response = apiGet("/services/$SERVICE_ID_0", "application/rdf+json", port)
        Assumptions.assumeTrue(HttpStatus.OK.value() == response["status"])

        val expected = responseReader.parseFile("service_0.ttl", "TURTLE")
        val responseModel = responseReader.parseResponse(response["body"] as String, "RDF/JSON")

        assertTrue(checkIfIsomorphicAndPrintDiff(actual = responseModel, expected = expected, name = "ServicesTest.findSpecific"))
    }

    @Test
    fun idDoesNotExist() {
        val response = apiGet("/services/123", "text/turtle", port)
        Assertions.assertEquals(HttpStatus.NOT_FOUND.value(), response["status"])
    }

}