/*
 * Copyright (c) 2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.s3_v2

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.annotation.JsonPropertyDescription
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaInject
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import io.airbyte.cdk.command.ConfigurationSpecification
import io.airbyte.cdk.load.command.aws.AWSAccessKeySpecification
import io.airbyte.cdk.load.command.aws.AWSArnRoleSpecification
import io.airbyte.cdk.load.command.object_storage.DeprecatedJsonFormatSpecification
import io.airbyte.cdk.load.command.object_storage.DeprecatedObjectStorageFormatSpecification
import io.airbyte.cdk.load.command.object_storage.DeprecatedObjectStorageFormatSpecificationProvider
import io.airbyte.cdk.load.command.s3.S3AddressingStyle
import io.airbyte.cdk.load.command.s3.S3BucketRegion
import io.airbyte.cdk.load.command.s3.S3BucketSpecification
import io.airbyte.cdk.load.command.s3.S3PathSpecification
import io.airbyte.cdk.load.spec.DestinationSpecificationExtension
import io.airbyte.protocol.models.v0.DestinationSyncMode
import jakarta.inject.Singleton

@Singleton
@JsonSchemaTitle("S3 V2 Destination Spec")
@JsonSchemaInject()
class S3V2Specification :
    ConfigurationSpecification(),
    AWSAccessKeySpecification,
    AWSArnRoleSpecification,
    S3BucketSpecification,
    S3PathSpecification,
    DeprecatedObjectStorageFormatSpecificationProvider {

    @get:JsonSchemaInject(
        json =
            """{"examples":["A012345678910EXAMPLE"],"airbyte_secret": true,"always_show": true,"order":0}"""
    )
    override val accessKeyId: String? = null

    @get:JsonSchemaInject(
        json =
            """{"examples":["a012345678910ABCDEFGH/AbCdEfGhEXAMPLEKEY"],"airbyte_secret": true,"always_show": true,"order":1}"""
    )
    override val secretAccessKey: String? = null

    @get:JsonSchemaInject(
        json =
            """{"examples":["arn:aws:iam::123456789:role/ExternalIdIsYourWorkspaceId"],"order":2}"""
    )
    override val roleArn: String? = null

    @get:JsonSchemaInject(json = """{"examples":["airbyte_sync"],"order":3}""")
    override val s3BucketName: String = ""

    @get:JsonSchemaInject(json = """{"examples":["data_sync/test"],"order":4}""")
    override val s3BucketPath: String = ""

    @get:JsonSchemaInject(json = """{"examples":["us-east-1"],"order":5,"default":""}""")
    override val s3BucketRegion: S3BucketRegion = S3BucketRegion.NO_REGION

    @get:JsonSchemaInject(json = """{"order":6}""")
    override val format: DeprecatedObjectStorageFormatSpecification =
        DeprecatedJsonFormatSpecification()

    @get:JsonSchemaInject(json = """{"examples":["http://localhost:9000"],"order":7}""")
    override val s3Endpoint: String? = null

    @get:JsonSchemaTitle("S3 Addressing Style")
    @get:JsonPropertyDescription(
        "Controls how the bucket name is sent to S3-compatible storage. Path style puts the bucket in the URL path. Virtual hosted style prefixes the bucket on the hostname and is required by some providers such as Alibaba OSS."
    )
    @get:JsonProperty("s3_addressing_style", defaultValue = "path_style")
    @get:JsonSchemaInject(json = """{"order":8,"default":"path_style"}""")
    override val s3AddressingStyle: S3AddressingStyle? = S3AddressingStyle.PATH_STYLE

    @get:JsonSchemaInject(
        json =
            "{\"examples\":[\"\${NAMESPACE}/\${STREAM_NAME}/\${YEAR}_\${MONTH}_\${DAY}_\${EPOCH}_\"],\"order\":9}"
    )
    override val s3PathFormat: String? = null

    @get:JsonSchemaInject(
        json =
            "{\"examples\":[\"{date}\",\"{date:yyyy_MM}\",\"{timestamp}\",\"{part_number}\",\"{sync_id}\"],\"order\":10}"
    )
    override val fileNamePattern: String? = null
}

@Singleton
class S3V2SpecificationExtension : DestinationSpecificationExtension {
    override val supportedSyncModes =
        listOf(
            DestinationSyncMode.OVERWRITE,
            DestinationSyncMode.APPEND,
        )
    override val supportsIncremental = true
}
