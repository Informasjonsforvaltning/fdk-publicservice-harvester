package no.fdk.fdk_public_service_harvester

import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.context.properties.ConfigurationPropertiesScan
import org.springframework.boot.runApplication
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity

@SpringBootApplication
@ConfigurationPropertiesScan
@EnableWebSecurity
open class Application

fun main(args: Array<String>) {
    SpringApplication.run(Application::class.java, *args)
}
