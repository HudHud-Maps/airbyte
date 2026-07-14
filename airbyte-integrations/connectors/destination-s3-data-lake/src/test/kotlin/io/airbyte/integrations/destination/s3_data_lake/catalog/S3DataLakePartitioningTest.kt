/*
 * Copyright (c) 2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.s3_data_lake.catalog

import io.airbyte.cdk.load.command.Append
import io.airbyte.cdk.load.command.Dedupe
import io.airbyte.cdk.load.command.ImportType
import io.airbyte.integrations.destination.s3_data_lake.spec.S3DataLakeConfiguration
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.apache.iceberg.PartitionSpec
import org.apache.iceberg.Schema
import org.apache.iceberg.Table
import org.apache.iceberg.types.Types
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

internal class S3DataLakePartitioningTest {

    private val schema =
        Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "country", Types.StringType.get()),
            Types.NestedField.optional(3, "updated_at", Types.TimestampType.withZone()),
        )

    private val dedupe = Dedupe(primaryKey = listOf(listOf("id")), cursor = listOf("updated_at"))

    private fun config(
        partitionMode: Boolean = false,
        partitionKeys: List<String> = emptyList(),
        autoDatePartition: Boolean = true,
        datePartitionColumn: String? = null,
    ): S3DataLakeConfiguration {
        val c = mockk<S3DataLakeConfiguration>()
        every { c.partitionMode } returns partitionMode
        every { c.partitionKeys } returns partitionKeys
        every { c.autoDatePartition } returns autoDatePartition
        every { c.datePartitionColumn } returns datePartitionColumn
        return c
    }

    private fun fieldNamesAndTransforms(spec: PartitionSpec): List<Pair<String, String>> =
        spec.fields().map { it.name() to it.transform().toString() }

    @Test
    fun `unpartitioned when nothing configured`() {
        val spec = buildPartitionSpec(schema, Append, config())
        assertTrue(spec.isUnpartitioned)
    }

    @Test
    fun `manual identity partitioning`() {
        val spec =
            buildPartitionSpec(
                schema,
                Append,
                config(partitionMode = true, partitionKeys = listOf("country")),
            )
        assertEquals(listOf("country" to "identity"), fieldNamesAndTransforms(spec))
    }

    @Test
    fun `manual identity partitioning skips missing columns`() {
        val spec =
            buildPartitionSpec(
                schema,
                Append,
                config(partitionMode = true, partitionKeys = listOf("country", "does_not_exist")),
            )
        assertEquals(listOf("country" to "identity"), fieldNamesAndTransforms(spec))
    }

    @Test
    fun `manual identity partitioning ignored when partition mode is off`() {
        val spec =
            buildPartitionSpec(
                schema,
                Append,
                config(partitionMode = false, partitionKeys = listOf("country")),
            )
        assertTrue(spec.isUnpartitioned)
    }

    @Test
    fun `auto date partitioning uses day transform on a temporal column for append`() {
        val spec =
            buildPartitionSpec(
                schema,
                Append,
                config(autoDatePartition = true, datePartitionColumn = "updated_at"),
            )
        assertEquals(listOf("updated_at_day" to "day"), fieldNamesAndTransforms(spec))
    }

    @Test
    fun `auto date partitioning skipped when column is not temporal`() {
        val spec =
            buildPartitionSpec(
                schema,
                Append,
                config(autoDatePartition = true, datePartitionColumn = "country"),
            )
        assertTrue(spec.isUnpartitioned)
    }

    @Test
    fun `auto date partitioning skipped when column is missing`() {
        val spec =
            buildPartitionSpec(
                schema,
                Append,
                config(autoDatePartition = true, datePartitionColumn = "missing"),
            )
        assertTrue(spec.isUnpartitioned)
    }

    @Test
    fun `auto date partitioning skipped for dedup streams`() {
        val spec =
            buildPartitionSpec(
                schema,
                dedupe,
                config(autoDatePartition = true, datePartitionColumn = "updated_at"),
            )
        assertTrue(spec.isUnpartitioned)
    }

    @Test
    fun `auto date partitioning skipped when disabled`() {
        val spec =
            buildPartitionSpec(
                schema,
                Append,
                config(autoDatePartition = false, datePartitionColumn = "updated_at"),
            )
        assertTrue(spec.isUnpartitioned)
    }

    @Test
    fun `manual identity for dedup streams is still applied`() {
        val spec =
            buildPartitionSpec(
                schema,
                dedupe,
                config(partitionMode = true, partitionKeys = listOf("country")),
            )
        assertEquals(listOf("country" to "identity"), fieldNamesAndTransforms(spec))
    }

    @Test
    fun `combined manual identity and auto date partitioning`() {
        val spec =
            buildPartitionSpec(
                schema,
                Append,
                config(
                    partitionMode = true,
                    partitionKeys = listOf("country"),
                    autoDatePartition = true,
                    datePartitionColumn = "updated_at",
                ),
            )
        assertEquals(
            listOf("country" to "identity", "updated_at_day" to "day"),
            fieldNamesAndTransforms(spec),
        )
    }

    @Test
    fun `date column already used as identity key is not partitioned twice`() {
        val spec =
            buildPartitionSpec(
                schema,
                Append,
                config(
                    partitionMode = true,
                    partitionKeys = listOf("updated_at"),
                    autoDatePartition = true,
                    datePartitionColumn = "updated_at",
                ),
            )
        assertEquals(listOf("updated_at" to "identity"), fieldNamesAndTransforms(spec))
    }

    @Test
    fun `applyPartitionSpec is a no-op when the table spec already matches`() {
        val desired =
            buildPartitionSpec(
                schema,
                Append,
                config(partitionMode = true, partitionKeys = listOf("country")),
            )
        val table = mockk<Table>()
        every { table.spec() } returns desired

        applyPartitionSpec(table, desired)

        verify(exactly = 0) { table.updateSpec() }
    }
}
