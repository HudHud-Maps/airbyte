/*
 * Copyright (c) 2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.s3_data_lake.catalog

import io.airbyte.cdk.load.command.Dedupe
import io.airbyte.cdk.load.command.ImportType
import io.airbyte.integrations.destination.s3_data_lake.spec.S3DataLakeConfiguration
import io.github.oshai.kotlinlogging.KotlinLogging
import org.apache.iceberg.PartitionSpec
import org.apache.iceberg.Schema
import org.apache.iceberg.Table
import org.apache.iceberg.expressions.Expressions
import org.apache.iceberg.expressions.Term
import org.apache.iceberg.types.Type

/**
 * Builds and applies Iceberg [PartitionSpec]s for the S3 Data Lake destination.
 *
 * Kept in its own file (rather than inside [S3DataLakeUtil]) to minimize merge conflicts when
 * syncing from airbytehq/airbyte -- the only change required elsewhere is a small block in
 * [io.airbyte.integrations.destination.s3_data_lake.write.S3DataLakeStreamLoader].
 *
 * Two partitioning strategies are supported:
 * - Manual identity partitioning ([S3DataLakeConfiguration.partitionMode] +
 * [S3DataLakeConfiguration.partitionKeys]): applies to every sync mode. For Append + Dedup streams
 * the operator must ensure the chosen columns are stable for a given primary key, otherwise updates
 * can leave stale rows in old partitions (equality deletes are partition-scoped).
 * - Auto date partitioning ([S3DataLakeConfiguration.autoDatePartition] +
 * [S3DataLakeConfiguration.datePartitionColumn]): applies to Append/Overwrite streams only, never to
 * Dedup. Uses Iceberg's native `day` transform on the configured temporal column. Native Iceberg
 * disallows multiple time-granularity transforms (year/month/day) on the same source column -- they
 * share a dedup name and throw "Cannot add redundant partition" -- so a single `day` transform is
 * used, which still enables year/month/day query pruning.
 */
private val logger = KotlinLogging.logger {}

/** Iceberg type ids that the `day` partition transform accepts. */
private fun Type.isTemporal(): Boolean =
    typeId() == Type.TypeID.DATE || typeId() == Type.TypeID.TIMESTAMP

/**
 * Compute the desired [PartitionSpec] for a stream from the connector configuration. Returns the
 * unpartitioned spec when nothing is configured (or nothing resolves).
 */
fun buildPartitionSpec(
    schema: Schema,
    importType: ImportType,
    config: S3DataLakeConfiguration,
): PartitionSpec {
    val builder = PartitionSpec.builderFor(schema)
    val usedSources = mutableSetOf<String>()

    // 1. Manual identity partitioning -- all sync modes.
    if (config.partitionMode && config.partitionKeys.isNotEmpty()) {
        config.partitionKeys.distinct().forEach { key ->
            when {
                schema.findField(key) == null ->
                    logger.warn {
                        "Partition key '$key' not found in schema; skipping identity partition."
                    }
                !usedSources.add(key) -> {
                    /* already added */
                }
                else -> builder.identity(key)
            }
        }
    }

    // 2. Auto date partitioning -- Append/Overwrite only, never Dedup.
    val dateColumn = config.datePartitionColumn
    if (config.autoDatePartition && dateColumn != null && importType !is Dedupe) {
        val field = schema.findField(dateColumn)
        when {
            field == null ->
                logger.warn {
                    "Date partition column '$dateColumn' not found in schema; skipping date partitioning."
                }
            !field.type().isTemporal() ->
                logger.warn {
                    "Date partition column '$dateColumn' is not a date/timestamp type " +
                        "(${field.type()}); skipping date partitioning."
                }
            !usedSources.add(dateColumn) ->
                logger.warn {
                    "Date partition column '$dateColumn' is already used as an identity partition " +
                        "key; skipping date partitioning."
                }
            else -> builder.day(dateColumn)
        }
    }

    return builder.build()
}

/**
 * Evolve [table]'s partition spec toward [desired] using metadata-only partition evolution. Existing
 * data files keep their layout. Best-effort: any failure is logged and the sync continues with the
 * table's existing spec.
 */
fun applyPartitionSpec(table: Table, desired: PartitionSpec) {
    val current = table.spec()
    if (current.compatibleWith(desired)) {
        return
    }
    try {
        val update = table.updateSpec()
        current.fields().forEach { field -> update.removeField(field.name()) }
        desired.fields().forEach { field ->
            val sourceName = desired.schema().findColumnName(field.sourceId())
            update.addField(field.name(), termFor(sourceName, field.transform().toString()))
        }
        update.commit()
        // Refresh so callers holding this Table see the new spec (used to select writers).
        table.refresh()
        logger.info {
            "Applied partition spec on ${table.name()}: ${desired.fields().map { it.name() }}"
        }
    } catch (e: Exception) {
        logger.warn(e) {
            "Failed to apply partition spec on ${table.name()}; continuing with the existing spec."
        }
    }
}

/**
 * Reconstruct a partition [Term] from a source column and the transform's canonical string form,
 * used when re-adding fields during partition evolution. Only the transforms produced by
 * [buildPartitionSpec] are handled; everything else falls back to identity.
 */
private fun termFor(source: String, transform: String): Term =
    when (transform) {
        "day" -> Expressions.day<Any>(source)
        "month" -> Expressions.month<Any>(source)
        "year" -> Expressions.year<Any>(source)
        "hour" -> Expressions.hour<Any>(source)
        else -> Expressions.ref<Any>(source)
    }
