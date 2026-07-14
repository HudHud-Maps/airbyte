/*
 * Copyright (c) 2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.s3_data_lake.spec

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.annotation.JsonPropertyDescription
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaDescription
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaInject
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import io.airbyte.cdk.command.ConfigurationSpecification
import io.airbyte.cdk.load.command.aws.AWSAccessKeySpecification
import io.airbyte.cdk.load.command.iceberg.parquet.CatalogType
import io.airbyte.cdk.load.command.iceberg.parquet.GlueCatalogSpecification
import io.airbyte.cdk.load.command.iceberg.parquet.IcebergCatalogSpecifications
import io.airbyte.cdk.load.spec.DestinationSpecificationExtension
import io.airbyte.protocol.models.v0.DestinationSyncMode
import jakarta.inject.Singleton

@Singleton
@JsonSchemaTitle("Iceberg V2 Destination Specification")
class S3DataLakeSpecification :
    ConfigurationSpecification(),
    AWSAccessKeySpecification,
    S3BucketSpecification,
    IcebergCatalogSpecifications {

    @get:JsonSchemaTitle("AWS Access Key ID")
    @get:JsonPropertyDescription(
        "The AWS Access Key ID with permissions for S3 and Glue operations."
    )
    @get:JsonSchemaInject(json = """{"airbyte_secret": true, "always_show": true, "order":0}""")
    override val accessKeyId: String? = null

    @get:JsonSchemaTitle("AWS Secret Access Key")
    @get:JsonPropertyDescription(
        "The AWS Secret Access Key paired with the Access Key ID for AWS authentication."
    )
    @get:JsonSchemaInject(json = """{"airbyte_secret": true, "always_show": true, "order":1}""")
    override val secretAccessKey: String? = null

    @get:JsonSchemaTitle("S3 Bucket Name")
    @get:JsonPropertyDescription("The name of the S3 bucket that will host the Iceberg data.")
    @get:JsonSchemaInject(json = """{"always_show": true,"order":2}""")
    override val s3BucketName: String = ""

    @get:JsonSchemaInject(
        json = """{"always_show": true,"examples":["me-central-1"], "default": "me-central-1", "order":3}"""
    )
    override val s3BucketRegion: S3BucketRegion = S3BucketRegion.`me-central-1`

    @get:JsonSchemaInject(json = """{"order":4}""") override val s3Endpoint: String? = null

    @get:JsonSchemaDescription(
        """The root location of the data warehouse used by the Iceberg catalog. Typically includes a bucket name and path within that bucket. For AWS Glue and Nessie, must include the storage protocol (such as "s3://" for Amazon S3)."""
    )
    @get:JsonSchemaInject(
        json =
            """
                {
                    "examples": ["s3://your-bucket/path/to/store/files/in"],
                    "always_show": true,
                    "order":5
                }
            """
    )
    override val warehouseLocation: String = ""

    @get:JsonSchemaInject(json = """{"always_show": true,"order":6}""")
    override val mainBranchName: String = ""

    @get:JsonSchemaInject(json = """{"always_show": true,"order":7}""")
    override val catalogType: CatalogType = GlueCatalogSpecification(glueId = "", databaseName = "")

    @get:JsonSchemaTitle("Flush Batch Size (MB)")
    @get:JsonPropertyDescription(
        "The approximate size in megabytes of each batch of data written to Iceberg. " +
            "Smaller values flush more frequently, improving data freshness and reducing data loss on failure, " +
            "but will create more small files that require compaction. " +
            "Must be between 1 and 500 MB. Default is 200 MB."
    )
    @get:JsonProperty("flush_batch_size_mb", required = false)
    @get:JsonSchemaInject(
        json = """{"examples":[200], "default": 200, "order": 8, "airbyte_hidden": true}"""
    )
    val flushBatchSizeMb: Long? = null

    @get:JsonSchemaTitle("Enable partition mode")
    @get:JsonPropertyDescription(
        "Enable partition mode to partition tables by the configured partition keys. " +
            "When enabled, tables are partitioned by identity on each column listed in Partition keys."
    )
    @get:JsonProperty("partition_mode", required = false)
    @get:JsonSchemaInject(json = """{"default": false, "order": 9}""")
    val partitionMode: Boolean? = null

    @get:JsonSchemaTitle("Partition keys")
    @get:JsonPropertyDescription(
        "List of column names to use as identity partition keys. Only used when partition mode is " +
            "enabled. Columns absent from a given stream are skipped. For Append + Dedup streams, " +
            "only list columns whose value is stable for a given primary key, otherwise updates can " +
            "leave stale rows in old partitions."
    )
    @get:JsonProperty("partition_keys", required = false)
    @get:JsonSchemaInject(json = """{"order": 10}""")
    val partitionKeys: List<String>? = null

    @get:JsonSchemaTitle("Auto date partition")
    @get:JsonPropertyDescription(
        "When enabled (default), automatically partitions Append/Overwrite streams by " +
            "year/month/day derived from the configured Date partition column. Only applies when " +
            "Date partition column is set and resolves to a date/timestamp type; it never applies to " +
            "Append + Dedup streams."
    )
    @get:JsonProperty("auto_date_partition", required = false)
    @get:JsonSchemaInject(json = """{"default": true, "order": 11}""")
    val autoDatePartition: Boolean? = null

    @get:JsonSchemaTitle("Date partition column")
    @get:JsonPropertyDescription(
        "The date/timestamp column used to derive year/month/day partitions when Auto date " +
            "partition is enabled. Must be a date or timestamp column; if it is missing or not a " +
            "temporal type, date partitioning is skipped. Ignored for Append + Dedup streams."
    )
    @get:JsonProperty("date_partition_column", required = false)
    @get:JsonSchemaInject(json = """{"order": 12}""")
    val datePartitionColumn: String? = null

    @get:JsonSchemaTitle("Namespace delimiter")
    @get:JsonPropertyDescription(
        "Optional. If set, the destination namespace is split on this delimiter into a multi-level " +
            "Iceberg namespace (for example, delimiter \".\" turns \"poi.bronze\" into namespace " +
            "[poi, bronze]). Leave empty to keep the namespace as a single level. Only supported for " +
            "REST, Nessie, and Polaris catalogs; must be empty for AWS Glue and Hive."
    )
    @get:JsonProperty("namespace_delimiter", required = false)
    @get:JsonSchemaInject(json = """{"examples":["."], "order": 13}""")
    val namespaceDelimiter: String? = null
}

@Singleton
class S3DataLakeSpecificationExtension : DestinationSpecificationExtension {
    override val supportedSyncModes =
        listOf(
            DestinationSyncMode.OVERWRITE,
            DestinationSyncMode.APPEND,
            DestinationSyncMode.APPEND_DEDUP
        )
    override val supportsIncremental = true
}
