package no.fdk.fdk_public_service_harvester.contract

import no.fdk.fdk_public_service_harvester.utils.*
import no.fdk.fdk_public_service_harvester.utils.jwk.Access
import no.fdk.fdk_public_service_harvester.utils.jwk.JwtToken
import org.junit.jupiter.api.*
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.web.server.LocalServerPort
import org.springframework.http.HttpStatus
import org.springframework.test.context.ContextConfiguration
import kotlin.test.assertEquals
import kotlin.test.assertTrue

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@SpringBootTest(
    properties = ["spring.profiles.active=contract-test"],
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ContextConfiguration(initializers = [ApiTestContext.Initializer::class])
@Tag("contract")
class UpdateTest: ApiTestContext() {

    @LocalServerPort
    var port: Int = 0

    private val responseReader = TestResponseReader()

    @Test
    fun unauthorizedForNoToken() {
        val response = apiAuthorizedPost("/update/meta", null, port)

        assertEquals(HttpStatus.UNAUTHORIZED.value(), response["status"])
    }

    @Test
    fun forbiddenForNonSysAdminRole() {
        val response = apiAuthorizedPost("/update/meta", JwtToken(Access.ORG_WRITE).toString(), port)

        assertEquals(HttpStatus.FORBIDDEN.value(), response["status"])
    }

    @Test
    fun noChangesWhenRunOnCorrectMeta() {
        val services = apiGet("/public-services?catalogrecords=true", "text/turtle", port)
        val catalogs = apiGet("/public-services/catalogs?catalogrecords=true", "text/turtle", port)

        val response = apiAuthorizedPost("/update/meta", JwtToken(Access.ROOT).toString(), port)

        assertEquals(HttpStatus.OK.value(), response["status"])

        val expectedServices = responseReader.parseResponse(services["body"] as String, "TURTLE")
        val expectedCatalogs = responseReader.parseResponse(catalogs["body"] as String, "TURTLE")

        val servicesAfterUpdate = apiGet("/public-services?catalogrecords=true", "text/turtle", port)
        val catalogsAfterUpdate = apiGet("/public-services/catalogs?catalogrecords=true", "text/turtle", port)

        val actualServices = responseReader.parseResponse(servicesAfterUpdate["body"] as String, "TURTLE")
        val actualCatalogs = responseReader.parseResponse(catalogsAfterUpdate["body"] as String, "TURTLE")

        assertTrue(checkIfIsomorphicAndPrintDiff(expectedServices, actualServices, "UpdateMetaServices"))
        assertTrue(checkIfIsomorphicAndPrintDiff(expectedCatalogs, actualCatalogs, "UpdateMetaCatalogs"))
    }

}
