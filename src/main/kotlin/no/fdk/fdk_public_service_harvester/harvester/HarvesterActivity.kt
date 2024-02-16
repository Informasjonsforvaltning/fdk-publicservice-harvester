package no.fdk.fdk_public_service_harvester.harvester

import io.micrometer.core.instrument.Metrics
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import no.fdk.fdk_public_service_harvester.adapter.HarvestAdminAdapter
import no.fdk.fdk_public_service_harvester.model.HarvestAdminParameters
import no.fdk.fdk_public_service_harvester.rabbit.RabbitMQPublisher
import no.fdk.fdk_public_service_harvester.service.UpdateService
import org.slf4j.LoggerFactory
import org.springframework.boot.context.event.ApplicationReadyEvent
import org.springframework.context.event.EventListener
import org.springframework.stereotype.Service
import java.util.Calendar
import kotlin.time.measureTimedValue
import kotlin.time.toJavaDuration

private val LOGGER = LoggerFactory.getLogger(HarvesterActivity::class.java)

@Service
class HarvesterActivity(
    private val harvestAdminAdapter: HarvestAdminAdapter,
    private val harvester: PublicServicesHarvester,
    private val publisher: RabbitMQPublisher,
    private val updateService: UpdateService
) : CoroutineScope by CoroutineScope(Dispatchers.Default) {

    private val activitySemaphore = Semaphore(1)

    @EventListener
    fun fullHarvestOnStartup(event: ApplicationReadyEvent) =
        initiateHarvest(HarvestAdminParameters(null, null, null), false)

    fun initiateHarvest(params: HarvestAdminParameters, forceUpdate: Boolean) {
        if (params.harvestAllServices()) LOGGER.debug("starting harvest of all services, force update: $forceUpdate")
        else LOGGER.debug("starting harvest with parameters $params, force update: $forceUpdate")

        launch {
            try {
                activitySemaphore.withPermit {
                    harvestAdminAdapter.getDataSources(params)
                        .filter { it.dataType == "publicService" }
                        .filter { it.url != null }
                        .map { async {
                            val (report, timeElapsed) = measureTimedValue {
                                harvester.harvestServices(it, Calendar.getInstance(), forceUpdate)
                            }
                            Metrics.counter("harvest_count",
                                    "status", if (report?.harvestError == false) { "success" }  else { "error" },
                                    "type", "public-service",
                                    "force_update", "$forceUpdate",
                                    "datasource_id", it.id
                            ).increment()
                            if (report?.harvestError == false) {
                                Metrics.counter("harvest_changed_resources_count",
                                        "type", "public-service",
                                        "force_update", "$forceUpdate",
                                        "datasource_id", it.id
                                ).increment(report.changedResources.size.toDouble())
                                Metrics.counter("harvest_removed_resources_count",
                                        "type", "public-service",
                                        "force_update", "$forceUpdate",
                                        "datasource_id", it.id
                                ).increment(report.removedResources.size.toDouble())
                                Metrics.timer("harvest_time",
                                        "type", "public-service",
                                        "force_update", "$forceUpdate",
                                        "datasource_id", it.id).record(timeElapsed.toJavaDuration())
                            }
                            report
                        } }
                        .awaitAll()
                        .filterNotNull()
                        .also { updateService.updateMetaData() }
                        .also {
                            if (params.harvestAllServices()) LOGGER.debug("completed harvest with parameters $params, force update: $forceUpdate")
                            else LOGGER.debug("completed harvest of all catalogs, force update: $forceUpdate")
                        }
                        .run { publisher.send(this) }
                }
            } catch (ex: Exception) {
                LOGGER.error("harvest failure", ex)
            }
        }
    }
}
