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
        val all = apiGet("/events", "text/turtle", port)
        val catalog = apiGet("/events/$SERVICE_ID_0", "text/turtle", port)
        val infoModel = apiGet("/events/$SERVICE_ID_1", "text/turtle", port)

        val response = apiAuthorizedPost("/update/meta", JwtToken(Access.ROOT).toString(), port)

        assertEquals(HttpStatus.OK.value(), response["status"])

        val expectedAll = responseReader.parseResponse(all["body"] as String, "TURTLE")
        val expectedCatalog = responseReader.parseResponse(catalog["body"] as String, "TURTLE")
        val expectedInfoModel = responseReader.parseResponse(infoModel["body"] as String, "TURTLE")

        val allAfterUpdate = apiGet("/events", "text/turtle", port)
        val catalogAfterUpdate = apiGet("/events/$SERVICE_ID_0", "text/turtle", port)
        val infoModelAfterUpdate = apiGet("/events/$SERVICE_ID_1", "text/turtle", port)

        val actualAll = responseReader.parseResponse(allAfterUpdate["body"] as String, "TURTLE")
        val actualCatalog = responseReader.parseResponse(catalogAfterUpdate["body"] as String, "TURTLE")
        val actualInfoModel = responseReader.parseResponse(infoModelAfterUpdate["body"] as String, "TURTLE")

        assertTrue(checkIfIsomorphicAndPrintDiff(expectedAll, actualAll, "UpdateMetaAll"))
        assertTrue(checkIfIsomorphicAndPrintDiff(expectedCatalog, actualCatalog, "UpdateMetaCatalog"))
        assertTrue(checkIfIsomorphicAndPrintDiff(expectedInfoModel, actualInfoModel, "UpdateMetaInfo"))
    }

}
