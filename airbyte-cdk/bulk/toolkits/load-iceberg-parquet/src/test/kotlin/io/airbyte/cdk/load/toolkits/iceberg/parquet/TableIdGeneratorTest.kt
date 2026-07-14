/*
 * Copyright (c) 2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cdk.load.toolkits.iceberg.parquet

import io.airbyte.cdk.load.command.DestinationStream
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class TableIdGeneratorTest {

    @Test
    fun `tableIdOf without delimiter keeps namespace as a single level`() {
        val id = tableIdOf("poi.bronze", "widgets")
        assertArrayEquals(arrayOf("poi.bronze"), id.namespace().levels())
        assertEquals("widgets", id.name())
    }

    @Test
    fun `tableIdOf with null or empty delimiter keeps namespace as a single level`() {
        assertArrayEquals(
            arrayOf("poi.bronze"),
            tableIdOf("poi.bronze", "widgets", null).namespace().levels()
        )
        assertArrayEquals(
            arrayOf("poi.bronze"),
            tableIdOf("poi.bronze", "widgets", "").namespace().levels()
        )
    }

    @Test
    fun `tableIdOf splits into a multi-level namespace on the delimiter`() {
        val id = tableIdOf("poi.bronze", "widgets", ".")
        assertArrayEquals(arrayOf("poi", "bronze"), id.namespace().levels())
        assertEquals("widgets", id.name())
    }

    @Test
    fun `tableIdOf supports three or more levels`() {
        val id = tableIdOf("a.b.c", "widgets", ".")
        assertArrayEquals(arrayOf("a", "b", "c"), id.namespace().levels())
    }

    @Test
    fun `tableIdOf drops empty parts from leading, trailing and repeated delimiters`() {
        assertArrayEquals(
            arrayOf("poi", "bronze"),
            tableIdOf(".poi.bronze.", "widgets", ".").namespace().levels()
        )
        assertArrayEquals(
            arrayOf("poi", "bronze"),
            tableIdOf("poi..bronze", "widgets", ".").namespace().levels()
        )
    }

    @Test
    fun `tableIdOf falls back to the original namespace when splitting yields nothing`() {
        val id = tableIdOf("...", "widgets", ".")
        assertArrayEquals(arrayOf("..."), id.namespace().levels())
    }

    @Test
    fun `tableIdOf trims whitespace around parts`() {
        val id = tableIdOf("poi . bronze", "widgets", ".")
        assertArrayEquals(arrayOf("poi", "bronze"), id.namespace().levels())
    }

    @Test
    fun `SimpleTableIdGenerator splits the stream namespace when a delimiter is set`() {
        val generator = SimpleTableIdGenerator(configNamespace = "fallback", namespaceDelimiter = ".")
        val id = generator.toTableIdentifier(DestinationStream.Descriptor("poi.bronze", "widgets"))
        assertArrayEquals(arrayOf("poi", "bronze"), id.namespace().levels())
        assertEquals("widgets", id.name())
    }

    @Test
    fun `SimpleTableIdGenerator splits the config namespace when the stream namespace is null`() {
        val generator =
            SimpleTableIdGenerator(configNamespace = "poi.bronze", namespaceDelimiter = ".")
        val id = generator.toTableIdentifier(DestinationStream.Descriptor(null, "widgets"))
        assertArrayEquals(arrayOf("poi", "bronze"), id.namespace().levels())
    }

    @Test
    fun `SimpleTableIdGenerator keeps a single level when no delimiter is configured`() {
        val generator = SimpleTableIdGenerator(configNamespace = "fallback")
        val id = generator.toTableIdentifier(DestinationStream.Descriptor("poi.bronze", "widgets"))
        assertArrayEquals(arrayOf("poi.bronze"), id.namespace().levels())
    }
}
