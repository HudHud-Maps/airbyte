/*
 * Copyright (c) 2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cdk.load.toolkits.iceberg.parquet.io

import io.airbyte.cdk.load.command.DestinationStream
import io.airbyte.cdk.load.data.AirbyteValueCoercer
import io.airbyte.cdk.load.toolkits.iceberg.parquet.SimpleTableIdGenerator
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import io.mockk.verifyOrder
import org.apache.iceberg.catalog.Catalog
import org.apache.iceberg.catalog.Namespace
import org.apache.iceberg.catalog.SupportsNamespaces
import org.apache.iceberg.exceptions.AlreadyExistsException
import org.junit.jupiter.api.Test

class IcebergUtilCreateNamespaceTest {

    /** Iceberg catalogs that manage namespaces implement both interfaces. */
    private interface NamespacedCatalog : Catalog, SupportsNamespaces

    private fun icebergUtil(delimiter: String?): IcebergUtil =
        IcebergUtil(
            SimpleTableIdGenerator(configNamespace = "", namespaceDelimiter = delimiter),
            AirbyteValueCoercer(),
        )

    @Test
    fun `creates each ancestor parent-first for a multi-level namespace`() {
        val util = icebergUtil(".")
        val catalog = mockk<NamespacedCatalog>()
        every { catalog.namespaceExists(any()) } returns false
        every { catalog.createNamespace(any()) } returns Unit

        util.createNamespace(DestinationStream.Descriptor("poi.bronze", "widgets"), catalog)

        verifyOrder {
            catalog.createNamespace(Namespace.of("poi"))
            catalog.createNamespace(Namespace.of("poi", "bronze"))
        }
    }

    @Test
    fun `skips ancestors that already exist`() {
        val util = icebergUtil(".")
        val catalog = mockk<NamespacedCatalog>()
        every { catalog.namespaceExists(Namespace.of("poi")) } returns true
        every { catalog.namespaceExists(Namespace.of("poi", "bronze")) } returns false
        every { catalog.createNamespace(any()) } returns Unit

        util.createNamespace(DestinationStream.Descriptor("poi.bronze", "widgets"), catalog)

        verify(exactly = 0) { catalog.createNamespace(Namespace.of("poi")) }
        verify(exactly = 1) { catalog.createNamespace(Namespace.of("poi", "bronze")) }
    }

    @Test
    fun `single-level namespace creates exactly one namespace`() {
        val util = icebergUtil(null)
        val catalog = mockk<NamespacedCatalog>()
        every { catalog.namespaceExists(any()) } returns false
        every { catalog.createNamespace(any()) } returns Unit

        util.createNamespace(DestinationStream.Descriptor("poi.bronze", "widgets"), catalog)

        verify(exactly = 1) { catalog.createNamespace(Namespace.of("poi.bronze")) }
    }

    @Test
    fun `swallows AlreadyExistsException from concurrent creation`() {
        val util = icebergUtil(".")
        val catalog = mockk<NamespacedCatalog>()
        every { catalog.namespaceExists(any()) } returns false
        every { catalog.createNamespace(Namespace.of("poi")) } throws
            AlreadyExistsException("already there")
        every { catalog.createNamespace(Namespace.of("poi", "bronze")) } returns Unit

        // should not throw
        util.createNamespace(DestinationStream.Descriptor("poi.bronze", "widgets"), catalog)

        verify(exactly = 1) { catalog.createNamespace(Namespace.of("poi", "bronze")) }
    }
}
