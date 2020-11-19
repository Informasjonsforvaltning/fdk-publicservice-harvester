package no.fdk.fdk_public_service_harvester.service

import com.nhaarman.mockitokotlin2.mock
import com.nhaarman.mockitokotlin2.whenever
import no.fdk.fdk_public_service_harvester.model.MiscellaneousTurtle
import no.fdk.fdk_public_service_harvester.model.UNION_ID
import no.fdk.fdk_public_service_harvester.rdf.JenaType
import no.fdk.fdk_public_service_harvester.repository.PublicServicesRepository
import no.fdk.fdk_public_service_harvester.repository.MiscellaneousRepository
import no.fdk.fdk_public_service_harvester.utils.*
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import java.util.*
import kotlin.test.assertNull
import kotlin.test.assertTrue

@Tag("unit")
class PublicServicesServiceTest {
    private val publicServicesRepository: PublicServicesRepository = mock()
    private val miscRepository: MiscellaneousRepository = mock()
    private val service = PublicServicesService(publicServicesRepository, miscRepository)

    private val responseReader = TestResponseReader()

    @Nested
    internal inner class AllServices {

        @Test
        fun responseIsometricWithEmptyModelForEmptyDB() {
            whenever(miscRepository.findById(UNION_ID))
                .thenReturn(Optional.empty())

            val expected = responseReader.parseResponse("", "TURTLE")

            val responseTurtle = service.getAll(JenaType.TURTLE)
            val responseJsonLD = service.getAll(JenaType.JSON_LD)

            assertTrue(expected.isIsomorphicWith(responseReader.parseResponse(responseTurtle, "TURTLE")))
            assertTrue(expected.isIsomorphicWith(responseReader.parseResponse(responseJsonLD, "JSON-LD")))
        }

        @Test
        fun getAllHandlesTurtleAndOtherRDF() {
            val allCatalogs = MiscellaneousTurtle(
                id = UNION_ID,
                isHarvestedSource = false,
                turtle = gzip(javaClass.classLoader.getResource("all_services.ttl")!!.readText())
            )

            whenever(miscRepository.findById(UNION_ID))
                .thenReturn(Optional.of(allCatalogs))

            val expected = responseReader.parseFile("all_services.ttl", "TURTLE")

            val responseTurtle = service.getAll(JenaType.TURTLE)
            val responseN3 = service.getAll(JenaType.N3)
            val responseNTriples = service.getAll(JenaType.NTRIPLES)

            assertTrue(expected.isIsomorphicWith(responseReader.parseResponse(responseTurtle, "TURTLE")))
            assertTrue(expected.isIsomorphicWith(responseReader.parseResponse(responseN3, "N3")))
            assertTrue(expected.isIsomorphicWith(responseReader.parseResponse(responseNTriples, "N-TRIPLES")))
        }

    }

    @Nested
    internal inner class ServiceById {

        @Test
        fun responseIsNullWhenNoModelIsFound() {
            whenever(publicServicesRepository.findOneByFdkId("123"))
                .thenReturn(null)

            val response = service.getServiceById("123", JenaType.TURTLE)

            assertNull(response)
        }

        @Test
        fun responseIsIsomorphicWithExpectedModel() {
            whenever(publicServicesRepository.findOneByFdkId(SERVICE_ID_0))
                .thenReturn(SERVICE_DBO_0)

            val responseTurtle = service.getServiceById(SERVICE_ID_0, JenaType.TURTLE)
            val responseRDFXML = service.getServiceById(SERVICE_ID_0, JenaType.RDF_XML)

            val expected = responseReader.parseFile("service_0.ttl", "TURTLE")

            assertTrue(expected.isIsomorphicWith(responseReader.parseResponse(responseTurtle!!, "TURTLE")))
            assertTrue(expected.isIsomorphicWith(responseReader.parseResponse(responseRDFXML!!, "RDF/XML")))
        }

    }

}