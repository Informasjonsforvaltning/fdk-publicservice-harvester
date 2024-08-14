package no.fdk.fdk_public_service_harvester.service

import no.fdk.fdk_public_service_harvester.model.FdkIdAndUri
import no.fdk.fdk_public_service_harvester.model.HarvestReport
import no.fdk.fdk_public_service_harvester.model.PublicServiceMeta
import no.fdk.fdk_public_service_harvester.rabbit.RabbitMQPublisher
import no.fdk.fdk_public_service_harvester.repository.PublicServicesRepository
import no.fdk.fdk_public_service_harvester.utils.*
import org.apache.jena.riot.Lang
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.mock
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import org.springframework.web.server.ResponseStatusException
import kotlin.test.assertNull
import kotlin.test.assertTrue

@Tag("unit")
class PublicServicesServiceTest {
    private val repository: PublicServicesRepository = mock()
    private val publisher: RabbitMQPublisher = mock()
    private val turtleService: TurtleService = mock()
    private val service = PublicServicesService(repository, publisher, turtleService)

    private val responseReader = TestResponseReader()

    @Nested
    internal inner class AllServices {

        @Test
        fun responseIsometricWithEmptyModelForEmptyDB() {
            whenever(turtleService.getServiceUnion(true))
                .thenReturn(null)

            val expected = responseReader.parseResponse("", "TURTLE")

            val responseTurtle = service.getAllServices(Lang.TURTLE, true)
            val responseJsonLD = service.getAllServices(Lang.JSONLD, true)

            assertTrue(expected.isIsomorphicWith(responseReader.parseResponse(responseTurtle, "TURTLE")))
            assertTrue(expected.isIsomorphicWith(responseReader.parseResponse(responseJsonLD, "JSON-LD")))
        }

        @Test
        fun getAllHandlesTurtleAndOtherRDF() {
            whenever(turtleService.getServiceUnion(true))
                .thenReturn(javaClass.classLoader.getResource("all_catalogs.ttl")!!.readText())
            whenever(turtleService.getServiceUnion(false))
                .thenReturn(javaClass.classLoader.getResource("no_meta_all_services.ttl")!!.readText())

            val expected = responseReader.parseFile("all_catalogs.ttl", "TURTLE")
            val expectedNoRecords = responseReader.parseFile("no_meta_all_services.ttl", "TURTLE")

            val responseTurtle = service.getAllServices(Lang.TURTLE, false)
            val responseN3 = service.getAllServices(Lang.N3, true)
            val responseNTriples = service.getAllServices(Lang.NTRIPLES, true)

            assertTrue(expectedNoRecords.isIsomorphicWith(responseReader.parseResponse(responseTurtle, "TURTLE")))
            assertTrue(expected.isIsomorphicWith(responseReader.parseResponse(responseN3, "N3")))
            assertTrue(expected.isIsomorphicWith(responseReader.parseResponse(responseNTriples, "N-TRIPLES")))
        }

    }

    @Nested
    internal inner class ServiceById {

        @Test
        fun responseIsNullWhenNoModelIsFound() {
            whenever(turtleService.getPublicService("123", true))
                .thenReturn(null)

            val response = service.getServiceById("123", Lang.TURTLE, true)

            assertNull(response)
        }

        @Test
        fun responseIsIsomorphicWithExpectedModel() {
            whenever(turtleService.getPublicService(SERVICE_ID_0, true))
                .thenReturn(javaClass.classLoader.getResource("service_0.ttl")!!.readText())
            whenever(turtleService.getPublicService(SERVICE_ID_0, false))
                .thenReturn(javaClass.classLoader.getResource("no_meta_service_0.ttl")!!.readText())

            val responseTurtle = service.getServiceById(SERVICE_ID_0, Lang.TURTLE, true)
            val responseRDFXML = service.getServiceById(SERVICE_ID_0, Lang.RDFXML, false)

            val expected = responseReader.parseFile("service_0.ttl", "TURTLE")
            val expectedNoRecords = responseReader.parseFile("no_meta_service_0.ttl", "TURTLE")

            assertTrue(expected.isIsomorphicWith(responseReader.parseResponse(responseTurtle!!, "TURTLE")))
            assertTrue(expectedNoRecords.isIsomorphicWith(responseReader.parseResponse(responseRDFXML!!, "RDF/XML")))
        }

    }

    @Nested
    internal inner class RemoveServiceById {

        @Test
        fun throwsResponseStatusExceptionWhenNoMetaFoundInDB() {
            whenever(repository.findAllByFdkId("123"))
                .thenReturn(emptyList())

            assertThrows<ResponseStatusException> { service.removeService("123") }
        }

        @Test
        fun throwsExceptionWhenNoNonRemovedMetaFoundInDB() {
            whenever(repository.findAllByFdkId(SERVICE_ID_0))
                .thenReturn(listOf(SERVICE_META_0.copy(removed = true)))

            assertThrows<ResponseStatusException> { service.removeService(SERVICE_ID_0) }
        }

        @Test
        fun updatesMetaAndSendsRabbitReportWhenMetaIsFound() {
            whenever(repository.findAllByFdkId(SERVICE_ID_0))
                .thenReturn(listOf(SERVICE_META_0))

            service.removeService(SERVICE_ID_0)

            argumentCaptor<List<PublicServiceMeta>>().apply {
                verify(repository, times(1)).saveAll(capture())
                assertEquals(listOf(SERVICE_META_0.copy(removed = true)), firstValue)
            }

            val expectedReport = HarvestReport(
                id = "manual-delete-$SERVICE_ID_0",
                url = SERVICE_META_0.uri,
                harvestError = false,
                startTime = "startTime",
                endTime = "endTime",
                removedResources = listOf(FdkIdAndUri(SERVICE_META_0.fdkId, SERVICE_META_0.uri))
            )
            argumentCaptor<List<HarvestReport>>().apply {
                verify(publisher, times(1)).send(capture())

                assertEquals(
                    listOf(expectedReport.copy(
                        startTime = firstValue.first().startTime,
                        endTime = firstValue.first().endTime
                    )),
                    firstValue
                )
            }
        }

    }

}
