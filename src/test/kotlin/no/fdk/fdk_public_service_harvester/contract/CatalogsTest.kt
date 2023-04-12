package no.fdk.fdk_public_service_harvester.contract

import no.fdk.fdk_public_service_harvester.utils.*
import org.apache.jena.riot.Lang
import org.junit.jupiter.api.*
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.web.server.LocalServerPort
import org.springframework.http.HttpStatus
import org.springframework.test.context.ContextConfiguration
import kotlin.test.assertTrue

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@SpringBootTest(
    properties = ["spring.profiles.active=contract-test"],
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ContextConfiguration(initializers = [ApiTestContext.Initializer::class])
@Tag("contract")
class CatalogsTest: ApiTestContext() {

    @LocalServerPort
    var port: Int = 0

    private val responseReader = TestResponseReader()

    @Test
    fun findAllWithRecords() {
        val response = apiGet("/public-services/catalogs?catalogrecords=true", "text/turtle", port)
        Assumptions.assumeTrue(HttpStatus.OK.value() == response["status"])

        val expected = responseReader.parseFile("all_catalogs.ttl", "TURTLE")
        val responseModel = responseReader.parseResponse(response["body"] as String, "TURTLE")

        assertTrue(checkIfIsomorphicAndPrintDiff(actual = responseModel, expected = expected, name = "CatalogsTest.findAll"))
    }

    @Test
    fun findAllNoRecords() {
        val response = apiGet("/public-services/catalogs", "application/trig", port)
        Assumptions.assumeTrue(HttpStatus.OK.value() == response["status"])

        val expected = responseReader.parseFile("no_meta_all_catalogs.ttl", "TURTLE")
        val responseModel = responseReader.parseResponse(response["body"] as String, Lang.TRIG.name)

        assertTrue(checkIfIsomorphicAndPrintDiff(actual = responseModel, expected = expected, name = "CatalogsTest.findAll"))
    }

    @Test
    fun findSpecificWithRecords() {
        val response = apiGet("/public-services/catalogs/$CATALOG_ID_1?catalogrecords=true", "application/rdf+json", port)
        Assumptions.assumeTrue(HttpStatus.OK.value() == response["status"])

        val expected = responseReader.parseFile("catalog_1.ttl", "TURTLE")
        val responseModel = responseReader.parseResponse(response["body"] as String, "RDF/JSON")

        assertTrue(checkIfIsomorphicAndPrintDiff(actual = responseModel, expected = expected, name = "CatalogsTest.findSpecific"))
    }

    @Test
    fun findSpecificNoRecords() {
        val response = apiGet("/public-services/catalogs/$CATALOG_ID_1", "application/n-quads", port)
        Assumptions.assumeTrue(HttpStatus.OK.value() == response["status"])

        val expected = responseReader.parseFile("no_meta_catalog_1.ttl", "TURTLE")
        val responseModel = responseReader.parseResponse(response["body"] as String, Lang.NQUADS.name)

        assertTrue(checkIfIsomorphicAndPrintDiff(actual = responseModel, expected = expected, name = "CatalogsTest.findSpecific"))
    }

    @Test
    fun idDoesNotExist() {
        val response = apiGet("/public-services/catalogs/123", "text/turtle", port)
        Assertions.assertEquals(HttpStatus.NOT_FOUND.value(), response["status"])
    }

}
