package no.fdk.fdk_public_service_harvester.harvester

import no.fdk.fdk_public_service_harvester.adapter.OrganizationsAdapter
import no.fdk.fdk_public_service_harvester.adapter.ServicesAdapter
import no.fdk.fdk_public_service_harvester.configuration.ApplicationProperties
import no.fdk.fdk_public_service_harvester.model.CatalogMeta
import no.fdk.fdk_public_service_harvester.model.FdkIdAndUri
import no.fdk.fdk_public_service_harvester.model.HarvestReport
import no.fdk.fdk_public_service_harvester.model.PublicServiceMeta
import no.fdk.fdk_public_service_harvester.repository.CatalogRepository
import no.fdk.fdk_public_service_harvester.repository.PublicServicesRepository
import no.fdk.fdk_public_service_harvester.service.TurtleService
import no.fdk.fdk_public_service_harvester.utils.*
import org.apache.jena.rdf.model.Model
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.mockito.kotlin.*
import java.util.*
import kotlin.test.assertEquals

@Tag("unit")
class HarvesterTest {
    private val metaRepository: PublicServicesRepository = mock()
    private val catalogMetaRepository: CatalogRepository = mock()
    private val turtleService: TurtleService = mock()
    private val valuesMock: ApplicationProperties = mock()
    private val adapter: ServicesAdapter = mock()
    private val orgAdapter: OrganizationsAdapter = mock()

    private val harvester = PublicServicesHarvester(adapter, orgAdapter, metaRepository, catalogMetaRepository, turtleService, valuesMock)
    private val responseReader = TestResponseReader()

    @Test
    fun harvestDataSourceSavedWhenDBIsEmpty() {
        whenever(adapter.fetchServices(TEST_HARVEST_SOURCE))
            .thenReturn(responseReader.readFile("harvest_response_0.ttl"))
        whenever(valuesMock.publicServiceHarvesterUri)
            .thenReturn("http://localhost:5050/public-services")

        val report = harvester.harvestServices(TEST_HARVEST_SOURCE, TEST_HARVEST_DATE, false)

        argumentCaptor<Model, String>().apply {
            verify(turtleService, times(1)).saveAsHarvestSource(first.capture(), second.capture())
            assertTrue(first.firstValue.isIsomorphicWith(responseReader.parseFile("harvest_response_0.ttl", "TURTLE")))
            Assertions.assertEquals(TEST_HARVEST_SOURCE.url, second.firstValue)
        }

        argumentCaptor<Model, String, Boolean>().apply {
            verify(turtleService, times(4)).saveAsPublicService(first.capture(), second.capture(), third.capture())
            assertTrue(checkIfIsomorphicAndPrintDiff(first.allValues[2], responseReader.parseFile("no_meta_service_0.ttl", "TURTLE"), "harvestDataSourceSavedWhenDBIsEmpty-norecords0"))
            assertTrue(checkIfIsomorphicAndPrintDiff(first.allValues[1], responseReader.parseFile("no_meta_service_1.ttl", "TURTLE"), "harvestDataSourceSavedWhenDBIsEmpty-norecords1"))
            assertTrue(checkIfIsomorphicAndPrintDiff(first.allValues[0], responseReader.parseFile("no_meta_service_2.ttl", "TURTLE"), "harvestDataSourceSavedWhenDBIsEmpty-norecords2"))
            assertTrue(checkIfIsomorphicAndPrintDiff(first.allValues[3], responseReader.parseFile("no_meta_service_3.ttl", "TURTLE"), "harvestDataSourceSavedWhenDBIsEmpty-norecords3"))
            assertEquals(listOf(SERVICE_ID_2, SERVICE_ID_1, SERVICE_ID_0, SERVICE_ID_3), second.allValues)
            Assertions.assertEquals(listOf(false, false, false, false), third.allValues)
        }

        argumentCaptor<PublicServiceMeta>().apply {
            verify(metaRepository, times(4)).save(capture())
            assertEquals(listOf(SERVICE_META_2.copy(isPartOf = null), SERVICE_META_1.copy(isPartOf = null), SERVICE_META_0.copy(isPartOf = null), SERVICE_META_3.copy(isPartOf = null)), allValues)
        }

        argumentCaptor<CatalogMeta>().apply {
            verify(catalogMetaRepository, times(1)).save(capture())
            assertEquals(listOf(CATALOG_META_0), allValues)
        }

        argumentCaptor<Model, String, Boolean>().apply {
            verify(turtleService, times(1)).saveAsCatalog(first.capture(), second.capture(), third.capture())
            assertTrue(checkIfIsomorphicAndPrintDiff(first.allValues[0], responseReader.parseFile("no_meta_catalog_0.ttl", "TURTLE"), "harvestDataSourceSavedWhenDBIsEmpty-catalog"))
            assertEquals(listOf(CATALOG_ID_0), second.allValues)
            Assertions.assertEquals(listOf(false), third.allValues)
        }

        val expectedReport = HarvestReport(
            id="test-source",
            url="http://localhost:5050/fdk-public-service-publisher.ttl",
            dataType="publicService",
            harvestError=false,
            startTime = "2020-10-05 15:15:39 +0200",
            endTime = report!!.endTime,
            changedCatalogs = listOf(FdkIdAndUri(fdkId=CATALOG_ID_0, uri=CATALOG_META_0.uri)),
            changedResources = listOf(
                FdkIdAndUri(fdkId= SERVICE_ID_2, uri= SERVICE_META_2.uri), FdkIdAndUri(fdkId= SERVICE_ID_1, uri= SERVICE_META_1.uri),
                FdkIdAndUri(fdkId= SERVICE_ID_0, uri= SERVICE_META_0.uri), FdkIdAndUri(fdkId= SERVICE_ID_3, uri= SERVICE_META_3.uri))
        )

        assertEquals(expectedReport, report)
    }

    @Test
    fun harvestDataSourceNotPersistedWhenNoChangesFromDB() {
        val harvested = responseReader.readFile("harvest_response_0.ttl")
        whenever(adapter.fetchServices(TEST_HARVEST_SOURCE))
            .thenReturn(harvested)
        whenever(valuesMock.publicServiceHarvesterUri)
            .thenReturn("http://localhost:5050/public-services")
        whenever(turtleService.getHarvestSource(TEST_HARVEST_SOURCE.url!!))
            .thenReturn(harvested)

        val report = harvester.harvestServices(TEST_HARVEST_SOURCE, TEST_HARVEST_DATE, false)

        verify(turtleService, times(0)).saveAsHarvestSource(any(), any())
        verify(turtleService, times(0)).saveAsPublicService(any(), any(), any())
        verify(metaRepository, times(0)).save(any())

        val expectedReport = HarvestReport(
            id="test-source",
            url="http://localhost:5050/fdk-public-service-publisher.ttl",
            dataType="publicService",
            harvestError=false,
            startTime = "2020-10-05 15:15:39 +0200",
            endTime = report!!.endTime
        )

        assertEquals(expectedReport, report)
    }

    @Test
    fun noChangesIgnoredWhenForceUpdateIsTrue() {
        val harvested = responseReader.readFile("harvest_response_0.ttl")
        whenever(adapter.fetchServices(TEST_HARVEST_SOURCE))
            .thenReturn(harvested)
        whenever(valuesMock.publicServiceHarvesterUri)
            .thenReturn("http://localhost:5050/public-services")
        whenever(turtleService.getHarvestSource(TEST_HARVEST_SOURCE.url!!))
            .thenReturn(harvested)

        val report = harvester.harvestServices(TEST_HARVEST_SOURCE, TEST_HARVEST_DATE, true)

        verify(turtleService, times(1)).saveAsHarvestSource(any(), any())
        verify(turtleService, times(4)).saveAsPublicService(any(), any(), any())
        verify(metaRepository, times(4)).save(any())

        val expectedReport = HarvestReport(
            id="test-source",
            url="http://localhost:5050/fdk-public-service-publisher.ttl",
            dataType="publicService",
            harvestError=false,
            startTime = "2020-10-05 15:15:39 +0200",
            endTime = report!!.endTime,
            changedCatalogs = listOf(FdkIdAndUri(fdkId=CATALOG_ID_0, uri=CATALOG_META_0.uri)),
            changedResources=listOf(
                FdkIdAndUri(fdkId="31249174-df02-3746-9d61-59fc61b4c5f9", uri="http://public-service-publisher.fellesdatakatalog.digdir.no/services/3"),
                FdkIdAndUri(fdkId="6ce4e524-3226-3591-ad99-c026705d4259", uri="http://public-service-publisher.fellesdatakatalog.digdir.no/services/2"),
                FdkIdAndUri(fdkId="d5d0c07c-c14f-3741-9aa3-126960958cf0", uri="http://public-service-publisher.fellesdatakatalog.digdir.no/services/1"),
                FdkIdAndUri(fdkId="1fc38c3c-1c86-3161-a9a7-e443fd94d413", uri="https://raw.githubusercontent.com/Informasjonsforvaltning/cpsv-ap-no/develop/examples/exTjenesteDummy.ttl"))
        )

        assertEquals(expectedReport, report)
    }

    @Test
    fun onlyRelevantUpdatedWhenHarvestedFromDB() {
        whenever(adapter.fetchServices(TEST_HARVEST_SOURCE))
            .thenReturn(responseReader.readFile("harvest_response_0.ttl"))
        whenever(valuesMock.publicServiceHarvesterUri)
            .thenReturn("http://localhost:5050/public-services")
        whenever(turtleService.getHarvestSource(TEST_HARVEST_SOURCE.url!!))
            .thenReturn(responseReader.readFile("harvest_response_0_diff.ttl"))
        whenever(metaRepository.findById(SERVICE_META_0.uri))
            .thenReturn(Optional.of(SERVICE_META_0))
        whenever(metaRepository.findById(SERVICE_META_1.uri))
            .thenReturn(Optional.of(SERVICE_META_1))
        whenever(metaRepository.findById(SERVICE_META_2.uri))
            .thenReturn(Optional.of(SERVICE_META_2))
        whenever(metaRepository.findById(SERVICE_META_3.uri))
            .thenReturn(Optional.of(SERVICE_META_3))
        whenever(metaRepository.findAllByIsPartOf("http://localhost:5050/public-services/catalogs/${CATALOG_META_0.fdkId}"))
            .thenReturn(listOf(SERVICE_META_0, SERVICE_META_1, SERVICE_META_2, SERVICE_META_3, SERVICE_META_4))
        whenever(metaRepository.saveAll(listOf(SERVICE_META_4.copy(removed = true))))
            .thenReturn(listOf(SERVICE_META_4.copy(removed = true)))
        whenever(catalogMetaRepository.findById(CATALOG_META_0.uri))
            .thenReturn(Optional.of(CATALOG_META_0))
        whenever(turtleService.getPublicService(SERVICE_ID_0, false))
            .thenReturn(responseReader.readFile("no_meta_service_0_diff.ttl"))
        whenever(turtleService.getPublicService(SERVICE_ID_1, false))
            .thenReturn(responseReader.readFile("no_meta_service_1.ttl"))
        whenever(turtleService.getPublicService(SERVICE_ID_2, false))
            .thenReturn(responseReader.readFile("no_meta_service_2.ttl"))
        whenever(turtleService.getPublicService(SERVICE_ID_3, false))
            .thenReturn(responseReader.readFile("no_meta_service_3.ttl"))
        whenever(turtleService.getCatalog(CATALOG_ID_0, false))
            .thenReturn(responseReader.readFile("no_meta_catalog_0_diff.ttl"))

        val report = harvester.harvestServices(TEST_HARVEST_SOURCE, NEW_TEST_HARVEST_DATE, false)

        argumentCaptor<Model, String>().apply {
            verify(turtleService, times(1)).saveAsHarvestSource(first.capture(), second.capture())
            assertTrue(first.firstValue.isIsomorphicWith(responseReader.parseFile("harvest_response_0.ttl", "TURTLE")))
            Assertions.assertEquals(TEST_HARVEST_SOURCE.url, second.firstValue)
        }

        argumentCaptor<Model, String, Boolean>().apply {
            verify(turtleService, times(1)).saveAsPublicService(first.capture(), second.capture(), third.capture())
            assertTrue(checkIfIsomorphicAndPrintDiff(first.allValues[0], responseReader.parseFile("no_meta_service_0.ttl", "TURTLE"), "onlyRelevantUpdatedWhenHarvestedFromDB-norecords0"))
            assertEquals(listOf(SERVICE_ID_0), second.allValues)
            Assertions.assertEquals(listOf(false), third.allValues)
        }

        argumentCaptor<PublicServiceMeta>().apply {
            verify(metaRepository, times(1)).save(capture())
            assertEquals(SERVICE_META_0.copy(modified = NEW_TEST_HARVEST_DATE.timeInMillis), firstValue)
        }

        val expectedReport = HarvestReport(
            id="test-source",
            url="http://localhost:5050/fdk-public-service-publisher.ttl",
            dataType="publicService",
            harvestError=false,
            startTime = "2020-10-15 13:52:16 +0200",
            endTime = report!!.endTime,
            changedCatalogs = listOf(FdkIdAndUri(fdkId=CATALOG_ID_0, uri=CATALOG_META_0.uri)),
            changedResources = listOf(FdkIdAndUri(fdkId="d5d0c07c-c14f-3741-9aa3-126960958cf0", uri="http://public-service-publisher.fellesdatakatalog.digdir.no/services/1")),
            removedResources = listOf(FdkIdAndUri(fdkId="ef4ca382-ee65-3a92-be9e-40fd93da53bc", uri="http://test.no/services/0"))
        )

        assertEquals(expectedReport, report)
    }

    @Test
    fun harvestWithErrorsIsNotPersisted() {
        whenever(adapter.fetchServices(TEST_HARVEST_SOURCE))
            .thenReturn(responseReader.readFile("harvest_error_response.ttl"))
        whenever(valuesMock.publicServiceHarvesterUri)
            .thenReturn("http://localhost:5050/public-services")

        val report = harvester.harvestServices(TEST_HARVEST_SOURCE, TEST_HARVEST_DATE, false)

        verify(turtleService, times(0)).saveAsHarvestSource(any(), any())
        verify(turtleService, times(0)).saveAsPublicService(any(), any(), any())
        verify(metaRepository, times(0)).save(any())

        val expectedReport = HarvestReport(
            id="test-source",
            url="http://localhost:5050/fdk-public-service-publisher.ttl",
            dataType="publicService",
            harvestError=true,
            errorMessage = "[line: 4, col: 3 ] Undefined prefix: dct",
            startTime = "2020-10-05 15:15:39 +0200",
            endTime = report!!.endTime
        )

        assertEquals(expectedReport, report)
    }

    @Test
    fun harvestDataSourceWithCatalog() {
        whenever(adapter.fetchServices(TEST_HARVEST_SOURCE))
            .thenReturn(responseReader.readFile("harvest_response_1.ttl"))
        whenever(valuesMock.publicServiceHarvesterUri)
            .thenReturn("http://localhost:5050/public-services")

        val report = harvester.harvestServices(TEST_HARVEST_SOURCE, TEST_HARVEST_DATE, false)

        argumentCaptor<Model, String>().apply {
            verify(turtleService, times(1)).saveAsHarvestSource(first.capture(), second.capture())
            assertTrue(first.firstValue.isIsomorphicWith(responseReader.parseFile("harvest_response_1.ttl", "TURTLE")))
            Assertions.assertEquals(TEST_HARVEST_SOURCE.url, second.firstValue)
        }

        argumentCaptor<Model, String, Boolean>().apply {
            verify(turtleService, times(2)).saveAsCatalog(first.capture(), second.capture(), third.capture())
            assertTrue(checkIfIsomorphicAndPrintDiff(first.firstValue, responseReader.parseFile("no_meta_catalog_1.ttl", "TURTLE"), "harvestDataSourceWithCatalog"))
            assertEquals(CATALOG_ID_1, second.firstValue)
            Assertions.assertEquals(listOf(false, false), third.allValues)
        }

        verify(turtleService, times(2)).saveAsPublicService(any(), any(), any())
        verify(metaRepository, times(2)).save(any())
        verify(catalogMetaRepository, times(2)).save(any())

        val expectedReport = HarvestReport(
            id="test-source",
            url="http://localhost:5050/fdk-public-service-publisher.ttl",
            dataType="publicService",
            harvestError=false,
            startTime = "2020-10-05 15:15:39 +0200",
            endTime = report!!.endTime,
            changedCatalogs=listOf(
                FdkIdAndUri(fdkId=CATALOG_ID_1, uri="http://test.no/catalogs/0"),
                FdkIdAndUri(fdkId=CATALOG_ID_0, uri="http://localhost:5050/fdk-public-service-publisher.ttl#GeneratedCatalog")),
            changedResources = listOf(
                FdkIdAndUri(fdkId="ef4ca382-ee65-3a92-be9e-40fd93da53bc", uri="http://test.no/services/0"),
                FdkIdAndUri(fdkId="7baa248b-1a27-3c46-80cf-889882d6b894", uri="http://test.no/services/1"))
        )

        assertEquals(expectedReport, report)
    }

    @Test
    fun ableToHarvestEmptyCollection() {
        val prev = responseReader.readFile("harvest_response_0.ttl")
        val harvested = responseReader.readFile("harvest_response_empty.ttl")
        whenever(adapter.fetchServices(TEST_HARVEST_SOURCE))
            .thenReturn(harvested)
        whenever(valuesMock.publicServiceHarvesterUri)
            .thenReturn("http://localhost:5050/public-services")
        whenever(turtleService.getHarvestSource(TEST_HARVEST_SOURCE.url!!))
            .thenReturn(prev)
        whenever(metaRepository.findAllByIsPartOf("http://localhost:5050/public-services/catalogs/${CATALOG_META_0.fdkId}"))
            .thenReturn(listOf(SERVICE_META_0, SERVICE_META_1, SERVICE_META_2, SERVICE_META_3))
        whenever(metaRepository.saveAll(listOf(SERVICE_META_0.copy(removed = true), SERVICE_META_1.copy(removed = true), SERVICE_META_2.copy(removed = true), SERVICE_META_3.copy(removed = true))))
            .thenReturn(listOf(SERVICE_META_0.copy(removed = true), SERVICE_META_1.copy(removed = true), SERVICE_META_2.copy(removed = true), SERVICE_META_3.copy(removed = true)))

        val report = harvester.harvestServices(TEST_HARVEST_SOURCE, TEST_HARVEST_DATE, false)

        val expectedReport = HarvestReport(
            id="test-source",
            url="http://localhost:5050/fdk-public-service-publisher.ttl",
            dataType="publicService",
            harvestError=false,
            startTime = "2020-10-05 15:15:39 +0200",
            endTime = report!!.endTime,
            changedCatalogs = listOf(FdkIdAndUri(fdkId=CATALOG_ID_0, uri=CATALOG_META_0.uri)),
            removedResources = listOf(FdkIdAndUri(fdkId=SERVICE_ID_0, uri=SERVICE_META_0.uri), FdkIdAndUri(fdkId=SERVICE_ID_1, uri=SERVICE_META_1.uri), FdkIdAndUri(fdkId=SERVICE_ID_2, uri=SERVICE_META_2.uri), FdkIdAndUri(fdkId=SERVICE_ID_3, uri=SERVICE_META_3.uri))
        )
        assertEquals(expectedReport, report)
    }

    @Test
    fun earlierRemovedServiceWithNoChangesAddedToReport() {
        val harvested = responseReader.readFile("harvest_response_0.ttl")
        whenever(adapter.fetchServices(TEST_HARVEST_SOURCE))
            .thenReturn(harvested)
        whenever(turtleService.getHarvestSource(TEST_HARVEST_SOURCE.url!!))
            .thenReturn(responseReader.readFile("harvest_response_empty.ttl"))
        whenever(metaRepository.findById(SERVICE_META_0.uri))
            .thenReturn(Optional.of(SERVICE_META_0.copy(removed = true)))
        whenever(metaRepository.findById(SERVICE_META_1.uri))
            .thenReturn(Optional.of(SERVICE_META_1))
        whenever(metaRepository.findById(SERVICE_META_2.uri))
            .thenReturn(Optional.of(SERVICE_META_2))
        whenever(metaRepository.findById(SERVICE_META_3.uri))
            .thenReturn(Optional.of(SERVICE_META_3))
        whenever(metaRepository.findAllByIsPartOf("http://localhost:5050/public-services/catalogs/${CATALOG_META_0.fdkId}"))
            .thenReturn(listOf(SERVICE_META_0.copy(removed = true), SERVICE_META_1, SERVICE_META_2, SERVICE_META_3))
        whenever(turtleService.getPublicService(SERVICE_ID_0, withRecords = false))
            .thenReturn(responseReader.readFile("no_meta_service_0.ttl"))
        whenever(turtleService.getPublicService(SERVICE_ID_1, withRecords = false))
            .thenReturn(responseReader.readFile("no_meta_service_1.ttl"))
        whenever(turtleService.getPublicService(SERVICE_ID_2, withRecords = false))
            .thenReturn(responseReader.readFile("no_meta_service_2.ttl"))
        whenever(turtleService.getPublicService(SERVICE_ID_3, withRecords = false))
            .thenReturn(responseReader.readFile("no_meta_service_3.ttl"))

        whenever(valuesMock.publicServiceHarvesterUri)
            .thenReturn("http://localhost:5050/public-services")

        val report = harvester.harvestServices(TEST_HARVEST_SOURCE, TEST_HARVEST_DATE, false)

        argumentCaptor<PublicServiceMeta>().apply {
            verify(metaRepository, times(1)).save(capture())
            assertEquals(SERVICE_META_0, firstValue)
        }

        val expectedReport = HarvestReport(
            id="test-source",
            url="http://localhost:5050/fdk-public-service-publisher.ttl",
            dataType="publicService",
            harvestError=false,
            startTime = "2020-10-05 15:15:39 +0200",
            endTime = report!!.endTime,
            changedCatalogs=listOf(
                FdkIdAndUri(fdkId=CATALOG_ID_0, uri="http://localhost:5050/fdk-public-service-publisher.ttl#GeneratedCatalog")),
            changedResources = listOf(
                FdkIdAndUri(fdkId= SERVICE_ID_0, uri= SERVICE_META_0.uri))
        )

        assertEquals(expectedReport, report)
    }

}
