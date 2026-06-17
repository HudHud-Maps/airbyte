/*
 * Copyright (c) 2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cdk.load.pipeline.object_storage

import io.airbyte.cdk.load.config.DataChannelMedium
import io.airbyte.cdk.load.file.object_storage.RemoteObject
import io.airbyte.cdk.load.message.StreamKey
import io.airbyte.cdk.load.pipline.object_storage.ObjectLoaderOneShotUploaderStep
import io.airbyte.cdk.load.pipline.object_storage.ObjectLoaderPartFormatterStep
import io.airbyte.cdk.load.pipline.object_storage.ObjectLoaderPartLoaderStep
import io.airbyte.cdk.load.pipline.object_storage.ObjectLoaderPipeline
import io.airbyte.cdk.load.pipline.object_storage.ObjectLoaderUploadCompleterStep
import io.airbyte.cdk.load.pipline.object_storage.file.FileChunkStep
import io.airbyte.cdk.load.pipline.object_storage.file.ForwardFileRecordStep
import io.airbyte.cdk.load.pipline.object_storage.file.ProcessFileTaskLegacyStep
import io.airbyte.cdk.load.pipline.object_storage.file.RouteEventStep
import io.mockk.mockk
import kotlin.test.assertEquals
import org.junit.jupiter.api.Test

class ObjectLoaderPipelineTest {
    @Test
    fun `legacy file transfer takes precedence over include files catalog routing`() {
        val steps = steps(isFileTransfer = true, isLegacyFileTransfer = true)

        assertEquals(
            listOf(
                legacyProcessFileStep,
                recordUploadStep,
                recordCompleterStep,
            ),
            steps,
        )
    }

    @Test
    fun `include files catalog routing uses file pipeline when legacy file transfer is disabled`() {
        val steps = steps(isFileTransfer = true, isLegacyFileTransfer = false)

        assertEquals(
            listOf(
                routeEventStep,
                fileChunkStep,
                fileChunkUploader,
                fileCompleterStep,
                forwardFileRecordStep,
                fileRecordFormatStep,
                recordUploadStep,
                recordCompleterStep,
            ),
            steps,
        )
    }

    private fun steps(
        isFileTransfer: Boolean,
        isLegacyFileTransfer: Boolean,
        dataChannelMedium: DataChannelMedium = DataChannelMedium.STDIO,
    ) =
        ObjectLoaderPipeline.selectPipelineSteps<StreamKey, TestRemoteObject>(
            isFileTransfer = isFileTransfer,
            routeEventStep = routeEventStep,
            fileChunkStep = fileChunkStep,
            fileChunkUploader = fileChunkUploader,
            fileCompleterStep = fileCompleterStep,
            forwardFileRecordStep = forwardFileRecordStep,
            fileRecordFormatStep = fileRecordFormatStep,
            recordPartStep = recordPartStep,
            recordUploadStep = recordUploadStep,
            recordCompleterStep = recordCompleterStep,
            isLegacyFileTransfer = isLegacyFileTransfer,
            legacyProcessFileStep = legacyProcessFileStep,
            oneShotObjectLoaderStep = oneShotObjectLoaderStep,
            dataChannelMedium = dataChannelMedium,
        )

    private companion object {
        val routeEventStep = mockk<RouteEventStep>()
        val fileChunkStep = mockk<FileChunkStep<TestRemoteObject>>()
        val fileChunkUploader = mockk<ObjectLoaderPartLoaderStep<TestRemoteObject>>()
        val fileCompleterStep =
            mockk<ObjectLoaderUploadCompleterStep<StreamKey, TestRemoteObject>>()
        val forwardFileRecordStep = mockk<ForwardFileRecordStep<TestRemoteObject>>()
        val fileRecordFormatStep = mockk<ObjectLoaderPartFormatterStep>()
        val recordPartStep = mockk<ObjectLoaderPartFormatterStep>()
        val recordUploadStep = mockk<ObjectLoaderPartLoaderStep<TestRemoteObject>>()
        val recordCompleterStep =
            mockk<ObjectLoaderUploadCompleterStep<StreamKey, TestRemoteObject>>()
        val legacyProcessFileStep = mockk<ProcessFileTaskLegacyStep>()
        val oneShotObjectLoaderStep =
            mockk<ObjectLoaderOneShotUploaderStep<StreamKey, TestRemoteObject>>()
    }

    private data class TestRemoteObject(
        override val key: String = "",
        override val storageConfig: Unit = Unit,
    ) : RemoteObject<Unit>
}
