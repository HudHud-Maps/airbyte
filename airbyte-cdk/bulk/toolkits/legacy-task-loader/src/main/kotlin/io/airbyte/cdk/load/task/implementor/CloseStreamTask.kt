/*
 * Copyright (c) 2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cdk.load.task.implementor

import io.airbyte.cdk.load.command.DestinationStream
import io.airbyte.cdk.load.state.SyncManager
import io.airbyte.cdk.load.task.DestinationTaskLauncher
import io.airbyte.cdk.load.task.SelfTerminating
import io.airbyte.cdk.load.task.Task
import io.airbyte.cdk.load.task.TerminalCondition
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Singleton

class CloseStreamTask(
    private val syncManager: SyncManager,
    val streamDescriptor: DestinationStream.Descriptor,
    private val taskLauncher: DestinationTaskLauncher
) : Task {
    private val log = KotlinLogging.logger {}

    override val terminalCondition: TerminalCondition = SelfTerminating

    override suspend fun execute() {
        val streamManager = syncManager.getStreamManager(streamDescriptor)
        val hadNonzeroRecords = streamManager.hadNonzeroRecords()
        val streamLoader =
            if (hadNonzeroRecords) {
                syncManager.getOrAwaitStreamLoader(streamDescriptor)
            } else {
                syncManager.getStreamLoaderOrNull(streamDescriptor)
            }

        if (streamLoader == null) {
            log.info { "No stream loader opened for zero-record stream $streamDescriptor" }
        } else {
            streamLoader.close(hadNonzeroRecords = hadNonzeroRecords)
        }

        streamManager.markProcessingSucceeded()
        taskLauncher.handleStreamClosed()
    }
}

@Singleton
class CloseStreamTaskFactory(
    private val syncManager: SyncManager,
) {
    fun make(
        taskLauncher: DestinationTaskLauncher,
        streamDescriptor: DestinationStream.Descriptor,
    ): CloseStreamTask {
        return CloseStreamTask(syncManager, streamDescriptor, taskLauncher)
    }
}
