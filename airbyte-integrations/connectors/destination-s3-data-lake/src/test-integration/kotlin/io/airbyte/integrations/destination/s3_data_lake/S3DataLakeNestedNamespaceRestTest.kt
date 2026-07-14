/*
 * Copyright (c) 2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.s3_data_lake

import io.airbyte.cdk.command.ValidatedJsonUtils
import io.airbyte.cdk.load.command.DestinationStream
import io.airbyte.cdk.load.data.AirbyteValueCoercer
import io.airbyte.cdk.load.toolkits.iceberg.parquet.SimpleTableIdGenerator
import io.airbyte.cdk.load.toolkits.iceberg.parquet.io.IcebergUtil
import io.airbyte.integrations.destination.s3_data_lake.catalog.S3DataLakeUtil
import io.airbyte.integrations.destination.s3_data_lake.spec.DEFAULT_CATALOG_NAME
import io.airbyte.integrations.destination.s3_data_lake.spec.S3DataLakeSpecification
import java.util.UUID
import org.apache.iceberg.catalog.Namespace
import org.apache.iceberg.catalog.SupportsNamespaces
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.parallel.Execution
import org.junit.jupiter.api.parallel.ExecutionMode

/**
 * Verifies that, against a real REST catalog (Lakekeeper-compatible), a destination namespace such
 * as "poi.bronze" with a "." delimiter lands in a genuine multi-level Iceberg namespace and that all
 * ancestor levels are created automatically.
 */
@Execution(ExecutionMode.SAME_THREAD)
class S3DataLakeNestedNamespaceRestTest {

    @Test
    fun `nested namespace is created parent-first on a REST catalog`() {
        val namespaceDelimiter = "."
        val topLevel = "poi_${UUID.randomUUID().toString().replace("-", "_")}"
        val childLevel = "bronze"
        val nestedNamespace = "$topLevel$namespaceDelimiter$childLevel"

        val spec =
            ValidatedJsonUtils.parseOne(
                S3DataLakeSpecification::class.java,
                getConfig(nestedNamespace, namespaceDelimiter),
            )
        val config = S3DataLakeTestUtil.getConfig(spec)
        val catalog = S3DataLakeTestUtil.getCatalog(config, null)

        val icebergUtil =
            IcebergUtil(
                SimpleTableIdGenerator(
                    configNamespace = nestedNamespace,
                    namespaceDelimiter = config.namespaceDelimiter,
                ),
                AirbyteValueCoercer(),
            )
        val s3DataLakeUtil = S3DataLakeUtil(icebergUtil, assumeRoleCredentials = null)

        val descriptor =
            DestinationStream.Descriptor(
                nestedNamespace,
                "widgets_${UUID.randomUUID().toString().replace("-", "_")}",
            )

        try {
            s3DataLakeUtil.createNamespaceWithGlueHandling(descriptor, catalog)

            catalog as SupportsNamespaces
            assertTrue(
                catalog.namespaceExists(Namespace.of(topLevel)),
                "expected ancestor namespace [$topLevel] to be auto-created",
            )
            assertTrue(
                catalog.namespaceExists(Namespace.of(topLevel, childLevel)),
                "expected nested namespace [$topLevel, $childLevel] to be created",
            )
        } finally {
            (catalog as? SupportsNamespaces)?.let { ns ->
                runCatching { ns.dropNamespace(Namespace.of(topLevel, childLevel)) }
                runCatching { ns.dropNamespace(Namespace.of(topLevel)) }
            }
        }
    }

    companion object {
        private fun getConfig(namespace: String, namespaceDelimiter: String): String {
            val minioEndpoint = RestTestContainers.testcontainers.getServiceHost("minio", 9000)
            val restEndpoint = RestTestContainers.testcontainers.getServiceHost("rest", 8181)
            return """
                {
                    "catalog_type": {
                      "catalog_type": "REST",
                      "server_uri": "http://$restEndpoint:8181",
                      "namespace": "$namespace"
                    },
                    "s3_bucket_name": "warehouse",
                    "s3_bucket_region": "us-east-1",
                    "access_key_id": "admin",
                    "secret_access_key": "password",
                    "s3_endpoint": "http://$minioEndpoint:9100",
                    "warehouse_location": "s3://warehouse/",
                    "main_branch_name": "main",
                    "namespace_delimiter": "$namespaceDelimiter"
                }
                """.trimIndent()
        }

        @JvmStatic
        @BeforeAll
        fun setup() {
            RestTestContainers.start()
        }

        @JvmStatic
        @AfterAll
        fun teardown() {
            // Containers are shared/singleton; leave them running for other REST tests.
        }
    }
}
