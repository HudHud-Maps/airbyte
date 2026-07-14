/*
 * Copyright (c) 2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cdk.load.toolkits.iceberg.parquet

import io.airbyte.cdk.load.command.DestinationStream
import org.apache.iceberg.catalog.Namespace
import org.apache.iceberg.catalog.TableIdentifier

/**
 * Convert our internal stream descriptor to an Iceberg [TableIdentifier]. Implementations should
 * handle catalog-specific naming restrictions.
 */
// TODO accept default namespace in config as a val here
interface TableIdGenerator {
    fun toTableIdentifier(stream: DestinationStream.Descriptor): TableIdentifier
}

class SimpleTableIdGenerator(
    private val configNamespace: String? = "",
    private val namespaceDelimiter: String? = null,
) : TableIdGenerator {
    override fun toTableIdentifier(stream: DestinationStream.Descriptor): TableIdentifier {
        val namespace = stream.namespace ?: configNamespace
        return tableIdOf(namespace!!, stream.name, namespaceDelimiter)
    }
}

// iceberg namespace+name must both be nonnull.
fun tableIdOf(namespace: String, name: String): TableIdentifier =
    TableIdentifier.of(Namespace.of(namespace), name)

/**
 * Build a [TableIdentifier], optionally splitting [namespace] into a multi-level Iceberg namespace.
 *
 * When [delimiter] is null or empty the namespace is treated as a single level (identical to the
 * two-arg [tableIdOf]). Otherwise the namespace is split on the literal [delimiter]; empty parts
 * (from leading/trailing/repeated delimiters) are dropped, and a namespace that splits to nothing
 * falls back to the original single-level namespace.
 */
fun tableIdOf(namespace: String, name: String, delimiter: String?): TableIdentifier {
    if (delimiter.isNullOrEmpty()) {
        return tableIdOf(namespace, name)
    }
    val parts =
        namespace
            .split(delimiter)
            .map { it.trim() }
            .filter { it.isNotEmpty() }
            .toTypedArray()
    val safeParts = if (parts.isEmpty()) arrayOf(namespace) else parts
    return TableIdentifier.of(Namespace.of(*safeParts), name)
}
