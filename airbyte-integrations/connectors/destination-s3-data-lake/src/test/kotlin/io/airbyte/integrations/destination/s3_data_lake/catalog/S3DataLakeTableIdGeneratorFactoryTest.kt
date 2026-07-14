/*
 * Copyright (c) 2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.s3_data_lake.catalog

import io.airbyte.cdk.ConfigErrorException
import io.airbyte.cdk.command.ValidatedJsonUtils
import io.airbyte.cdk.load.command.DestinationStream
import io.airbyte.cdk.load.command.aws.AWSAccessKeyConfiguration
import io.airbyte.cdk.load.command.aws.AWSArnRoleConfiguration
import io.airbyte.cdk.load.command.iceberg.parquet.GlueCatalogConfiguration
import io.airbyte.cdk.load.command.iceberg.parquet.IcebergCatalogConfiguration
import io.airbyte.cdk.load.command.iceberg.parquet.RestCatalogConfiguration
import io.airbyte.integrations.destination.s3_data_lake.spec.S3BucketConfiguration
import io.airbyte.integrations.destination.s3_data_lake.spec.S3BucketRegion
import io.airbyte.integrations.destination.s3_data_lake.spec.S3DataLakeConfiguration
import io.airbyte.integrations.destination.s3_data_lake.spec.S3DataLakeConfigurationFactory
import io.airbyte.integrations.destination.s3_data_lake.spec.S3DataLakeSpecification
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

class S3DataLakeTableIdGeneratorFactoryTest {

    private fun restConfig(namespaceDelimiter: String?): S3DataLakeConfiguration =
        S3DataLakeConfiguration(
            awsAccessKeyConfiguration =
                AWSAccessKeyConfiguration(accessKeyId = "k", secretAccessKey = "s"),
            s3BucketConfiguration =
                S3BucketConfiguration(
                    s3BucketName = "test",
                    s3BucketRegion = S3BucketRegion.`us-east-1`.region,
                    s3Endpoint = null,
                ),
            icebergCatalogConfiguration =
                IcebergCatalogConfiguration(
                    warehouseLocation = "s3://test/",
                    mainBranchName = "main",
                    catalogConfiguration =
                        RestCatalogConfiguration(
                            serverUri = "http://localhost:8181",
                            namespace = "poi.bronze",
                        ),
                ),
            flushBatchSizeMb = null,
            namespaceDelimiter = namespaceDelimiter,
        )

    private fun glueConfig(): S3DataLakeConfiguration =
        S3DataLakeConfiguration(
            awsAccessKeyConfiguration =
                AWSAccessKeyConfiguration(accessKeyId = "k", secretAccessKey = "s"),
            s3BucketConfiguration =
                S3BucketConfiguration(
                    s3BucketName = "test",
                    s3BucketRegion = S3BucketRegion.`us-east-1`.region,
                    s3Endpoint = null,
                ),
            icebergCatalogConfiguration =
                IcebergCatalogConfiguration(
                    warehouseLocation = "s3://test/",
                    mainBranchName = "main",
                    catalogConfiguration =
                        GlueCatalogConfiguration(
                            glueId = "123456789012",
                            awsArnRoleConfiguration = AWSArnRoleConfiguration(roleArn = null),
                            databaseName = "poi.bronze",
                        ),
                ),
            flushBatchSizeMb = null,
        )

    @Test
    fun `REST catalog with delimiter produces a splitting generator`() {
        val generator = TableIdGeneratorFactory(restConfig(".")).create()
        val id = generator.toTableIdentifier(DestinationStream.Descriptor("poi.bronze", "widgets"))
        assertArrayEquals(arrayOf("poi", "bronze"), id.namespace().levels())
        assertEquals("widgets", id.name())
    }

    @Test
    fun `REST catalog without delimiter keeps a single-level namespace`() {
        val generator = TableIdGeneratorFactory(restConfig(null)).create()
        val id = generator.toTableIdentifier(DestinationStream.Descriptor("poi.bronze", "widgets"))
        assertArrayEquals(arrayOf("poi.bronze"), id.namespace().levels())
    }

    @Test
    fun `Glue catalog ignores the delimiter and flattens the namespace`() {
        val generator = TableIdGeneratorFactory(glueConfig()).create()
        assertTrue(generator is GlueTableIdGenerator)
        val id = generator.toTableIdentifier(DestinationStream.Descriptor("poi.bronze", "Widgets"))
        // Glue lowercases and replaces non-alphanumerics with "_" - still a single level.
        assertArrayEquals(arrayOf("poi_bronze"), id.namespace().levels())
        assertEquals("widgets", id.name())
    }

    @Test
    fun `config validation rejects namespace delimiter for Glue catalogs`() {
        val json =
            """
            {
                "catalog_type": {
                  "catalog_type": "GLUE",
                  "glue_id": "123456789012",
                  "database_name": "poi"
                },
                "s3_bucket_name": "warehouse",
                "s3_bucket_region": "us-east-1",
                "access_key_id": "admin",
                "secret_access_key": "password",
                "warehouse_location": "s3://warehouse/",
                "main_branch_name": "main",
                "namespace_delimiter": "."
            }
            """.trimIndent()
        val spec = ValidatedJsonUtils.parseOne(S3DataLakeSpecification::class.java, json)
        assertThrows<ConfigErrorException> {
            S3DataLakeConfigurationFactory().makeWithoutExceptionHandling(spec)
        }
    }

    @Test
    fun `config validation accepts namespace delimiter for REST catalogs`() {
        val json =
            """
            {
                "catalog_type": {
                  "catalog_type": "REST",
                  "server_uri": "http://localhost:8181",
                  "namespace": "poi.bronze"
                },
                "s3_bucket_name": "warehouse",
                "s3_bucket_region": "us-east-1",
                "access_key_id": "admin",
                "secret_access_key": "password",
                "warehouse_location": "s3://warehouse/",
                "main_branch_name": "main",
                "namespace_delimiter": "."
            }
            """.trimIndent()
        val spec = ValidatedJsonUtils.parseOne(S3DataLakeSpecification::class.java, json)
        val config = S3DataLakeConfigurationFactory().makeWithoutExceptionHandling(spec)
        assertEquals(".", config.namespaceDelimiter)
    }
}
