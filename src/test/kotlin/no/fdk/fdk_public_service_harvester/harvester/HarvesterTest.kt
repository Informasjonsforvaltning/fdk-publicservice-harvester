package no.fdk.fdk_public_service_harvester.harvester

import com.nhaarman.mockitokotlin2.*
import no.fdk.fdk_public_service_harvester.adapter.FusekiAdapter
import no.fdk.fdk_public_service_harvester.adapter.ServicesAdapter
import no.fdk.fdk_public_service_harvester.configuration.ApplicationProperties
import no.fdk.fdk_public_service_harvester.model.PublicServiceDBO
import no.fdk.fdk_public_service_harvester.model.MiscellaneousTurtle
import no.fdk.fdk_public_service_harvester.repository.PublicServicesRepository
import no.fdk.fdk_public_service_harvester.repository.MiscellaneousRepository
import no.fdk.fdk_public_service_harvester.service.gzip
import no.fdk.fdk_public_service_harvester.utils.*
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertAll
import java.util.*
import kotlin.test.assertEquals

@Tag("unit")
class HarvesterTest {
    private val publicServicesRepository: PublicServicesRepository = mock()
    private val miscRepository: MiscellaneousRepository = mock()
    private val valuesMock: ApplicationProperties = mock()
    private val adapter: ServicesAdapter = mock()
    private val fusekiAdapter: FusekiAdapter = mock()

    private val harvester = PublicServicesHarvester(
        adapter, fusekiAdapter, publicServicesRepository, miscRepository, valuesMock
    )

    private val responseReader = TestResponseReader()

    @Test
    fun harvestDataSourceSavedWhenDBIsEmpty() {
        whenever(adapter.fetchServices(TEST_HARVEST_SOURCE))
            .thenReturn(responseReader.readFile("harvest_response_0.ttl"))
        whenever(valuesMock.publicServiceHarvesterUri)
            .thenReturn("http://localhost:5000/public-services")

        harvester.harvestServices(TEST_HARVEST_SOURCE, TEST_HARVEST_DATE)

        argumentCaptor<MiscellaneousTurtle>().apply {
            verify(miscRepository, times(1)).save(capture())
            HARVESTED_DBO.printTurtleDiff(firstValue)
            assertEquals(HARVESTED_DBO, firstValue)
        }

        argumentCaptor<List<PublicServiceDBO>>().apply {
            verify(publicServicesRepository, times(1)).saveAll(capture())
            val services = firstValue.sortedBy { it.uri }
            services[0].printTurtleDiff(SERVICE_DBO_0)
            services[1].printTurtleDiff(SERVICE_DBO_1)
            services[2].printTurtleDiff(SERVICE_DBO_2)
            assertAll("services",
                { assertEquals(3, services.size) },
                { assertEquals(SERVICE_DBO_0, services[0]) },
                { assertEquals(SERVICE_DBO_1, services[1]) },
                { assertEquals(SERVICE_DBO_2, services[2]) }
            )
        }
    }

    @Test
    fun harvestDataSourceNotPersistedWhenNoChangesFromDB() {
        whenever(adapter.fetchServices(TEST_HARVEST_SOURCE))
            .thenReturn(responseReader.readFile("harvest_response_0.ttl"))
        whenever(valuesMock.publicServiceHarvesterUri)
            .thenReturn("http://localhost:5000/public-services")

        whenever(miscRepository.findById(TEST_HARVEST_SOURCE.url!!))
            .thenReturn(Optional.of(HARVESTED_DBO))

        harvester.harvestServices(TEST_HARVEST_SOURCE, TEST_HARVEST_DATE)

        argumentCaptor<MiscellaneousTurtle>().apply {
            verify(miscRepository, times(0)).save(capture())
        }

        argumentCaptor<List<PublicServiceDBO>>().apply {
            verify(publicServicesRepository, times(0)).saveAll(capture())
        }

    }

    @Test
    fun onlyRelevantUpdatedWhenHarvestedFromDB() {
        whenever(adapter.fetchServices(TEST_HARVEST_SOURCE))
            .thenReturn(responseReader.readFile("harvest_response_0.ttl"))
        whenever(valuesMock.publicServiceHarvesterUri)
            .thenReturn("http://localhost:5000/public-services")

        val diffTurtle = gzip(responseReader.readFile("harvest_response_0_diff.ttl"))

        whenever(miscRepository.findById("http://localhost:5000/harvest0"))
            .thenReturn(Optional.of(HARVESTED_DBO.copy(turtle = diffTurtle)))

        whenever(publicServicesRepository.findById(SERVICE_DBO_0.uri))
            .thenReturn(Optional.of(SERVICE_DBO_0.copy(
                turtleHarvested = gzip(responseReader.readFile("no_meta_service_0_diff.ttl")),
                turtleService = gzip(responseReader.readFile("service_0_diff.ttl"))
            )))
        whenever(publicServicesRepository.findById(SERVICE_DBO_1.uri))
            .thenReturn(Optional.of(SERVICE_DBO_1))
        whenever(publicServicesRepository.findById(SERVICE_DBO_2.uri))
            .thenReturn(Optional.of(SERVICE_DBO_2))

        val expectedPublicServiceDBO = SERVICE_DBO_0.copy(
            modified = NEW_TEST_HARVEST_DATE.timeInMillis,
            turtleService = gzip(responseReader.readFile("service_0_modified.ttl"))
        )

        harvester.harvestServices(TEST_HARVEST_SOURCE, NEW_TEST_HARVEST_DATE)

        argumentCaptor<MiscellaneousTurtle>().apply {
            verify(miscRepository, times(1)).save(capture())
            firstValue.printTurtleDiff(HARVESTED_DBO)
            assertEquals(HARVESTED_DBO, firstValue)
        }

        argumentCaptor<List<PublicServiceDBO>>().apply {
            verify(publicServicesRepository, times(1)).saveAll(capture())
            assertEquals(1, firstValue.size)
            firstValue.first().printTurtleDiff(expectedPublicServiceDBO)
            assertEquals(expectedPublicServiceDBO, firstValue.first())
        }

    }

    @Test
    fun harvestWithErrorsIsNotPersisted() {
        whenever(adapter.fetchServices(TEST_HARVEST_SOURCE))
            .thenReturn(responseReader.readFile("harvest_error_response.ttl"))
        whenever(valuesMock.publicServiceHarvesterUri)
            .thenReturn("http://localhost:5000/public-services")

        harvester.harvestServices(TEST_HARVEST_SOURCE, TEST_HARVEST_DATE)

        argumentCaptor<MiscellaneousTurtle>().apply {
            verify(miscRepository, times(0)).save(capture())
        }

        argumentCaptor<List<PublicServiceDBO>>().apply {
            verify(publicServicesRepository, times(0)).saveAll(capture())
        }
    }

}