/*
 * Copyright (c) 2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cdk.load.task.implementor

import io.airbyte.cdk.load.command.DestinationStream
import io.airbyte.cdk.load.state.StreamManager
import io.airbyte.cdk.load.state.SyncManager
import io.airbyte.cdk.load.task.DestinationTaskLauncher
import io.airbyte.cdk.load.write.StreamLoader
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.every
import io.mockk.mockk
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test

class CloseStreamTaskTest {
    @Test
    fun `zero-record stream does not wait forever for unopened stream loader`() = runTest {
        val stream = DestinationStream.Descriptor(namespace = null, name = "geotag_raw_files")
        val syncManager = mockk<SyncManager>()
        val streamManager = mockk<StreamManager>()
        val taskLauncher = mockk<DestinationTaskLauncher>(relaxed = true)

        every { syncManager.getStreamManager(stream) } returns streamManager
        every { streamManager.hadNonzeroRecords() } returns false
        coEvery { syncManager.getStreamLoaderOrNull(stream) } returns null
        every { streamManager.markProcessingSucceeded() } returns Unit

        CloseStreamTask(syncManager, stream, taskLauncher).execute()

        coVerify(exactly = 0) { syncManager.getOrAwaitStreamLoader(stream) }
        coVerify { syncManager.getStreamLoaderOrNull(stream) }
        coVerify { taskLauncher.handleStreamClosed() }
    }

    @Test
    fun `nonzero stream still waits for stream loader before closing`() = runTest {
        val stream = DestinationStream.Descriptor(namespace = null, name = "geotag_raw_files")
        val syncManager = mockk<SyncManager>()
        val streamManager = mockk<StreamManager>()
        val streamLoader = mockk<StreamLoader>()
        val taskLauncher = mockk<DestinationTaskLauncher>(relaxed = true)

        every { syncManager.getStreamManager(stream) } returns streamManager
        every { streamManager.hadNonzeroRecords() } returns true
        coEvery { syncManager.getOrAwaitStreamLoader(stream) } returns streamLoader
        coEvery { streamLoader.close(hadNonzeroRecords = true) } returns Unit
        every { streamManager.markProcessingSucceeded() } returns Unit

        CloseStreamTask(syncManager, stream, taskLauncher).execute()

        coVerify { syncManager.getOrAwaitStreamLoader(stream) }
        coVerify(exactly = 0) { syncManager.getStreamLoaderOrNull(stream) }
        coVerify { streamLoader.close(hadNonzeroRecords = true) }
        coVerify { taskLauncher.handleStreamClosed() }
    }
}
