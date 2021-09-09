package org.apache.hadoop.ozone.container.lstore.helpers;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.hdds.utils.BatchOperation;
import org.apache.hadoop.hetu.photon.ClientTestUtil;
import org.apache.hadoop.hetu.photon.helpers.PartialRow;
import org.apache.hadoop.hetu.photon.operation.OperationType;
import org.apache.hadoop.hetu.photon.operation.request.InsertOperationRequest;
import org.apache.hadoop.hetu.photon.operation.request.OperationRequest;
import org.apache.hadoop.hetu.photon.operation.request.ScanQueryOperationRequest;
import org.apache.hadoop.hetu.photon.operation.response.OperationResponse;
import org.apache.hadoop.hetu.photon.proto.HetuPhotonProtos;
import org.apache.hadoop.ozone.common.ChunkBuffer;
import org.apache.hadoop.ozone.common.utils.BufferUtils;
import org.apache.hadoop.ozone.container.common.volume.VolumeIOStats;
import org.apache.hadoop.ozone.tablet.lstore.helpers.LStoreUtils;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.LambdaTestUtils;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.UNABLE_TO_FIND_CHUNK;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * Created by xiliu on 2021/5/24
 */
public class TestLStoreUtils {
    private static final Logger LOG =
            LoggerFactory.getLogger(TestLStoreUtils.class);

    private static final String PREFIX = TestLStoreUtils.class.getSimpleName();

    @Test
    public void concurrentReadOfSameFile() throws Exception {
        PartialRow raw = ClientTestUtil.getPartialRowWithAllTypes();
        OperationRequest operationRequest = OperationRequest.newBuilder()
                .setOperationType(OperationType.INSERT)
                .setInsertOperationRequest(new InsertOperationRequest(raw))
                .build();
        Assert.assertTrue(operationRequest.getOperationType().equals(OperationType.INSERT));
        byte[] array = operationRequest.toProto().toByteArray();
        ChunkBuffer data = ChunkBuffer.wrap(ByteBuffer.wrap(array));
        Path tempPath = Files.createTempDirectory(PREFIX);
        try {
            long len = data.limit();
            long offset = 0;
            VolumeIOStats stats = new VolumeIOStats();
            LStoreUtils.writeData(tempPath, data, offset, len, stats, true);
            int threads = 10;
            ExecutorService executor = new ThreadPoolExecutor(threads, threads,
                    0, TimeUnit.SECONDS, new LinkedBlockingQueue<>());
            AtomicInteger processed = new AtomicInteger();
            AtomicBoolean failed = new AtomicBoolean();
            for (int i = 0; i < threads; i++) {
                final int threadNumber = i;
                executor.execute(() -> {
                    try {
                        ByteBuffer[] readBuffers = BufferUtils.assignByteBuffers(len, len);
                        LStoreUtils.readData(tempPath, readBuffers, null, offset, len, stats);

                        // There should be only one element in readBuffers
                        Assert.assertEquals(1, readBuffers.length);
                        ByteBuffer readBuffer = readBuffers[0];

                        PartialRow partialRow = PartialRow.fromProtobuf(HetuPhotonProtos.PartialRowProto.parseFrom(readBuffer.array()));
                        LOG.info("Read data ({}): {}", threadNumber, partialRow.toString());
                        if (!Arrays.equals(operationRequest.getInsertOperationRequest().getRow().encodeColumnKey(),
                                partialRow.encodeColumnKey())) {
                            failed.set(true);
                        }
                        if (!Arrays.equals(raw.toProtobuf().toByteArray(), readBuffer.array())) {
                            failed.set(true);
                        }
                        assertEquals(operationRequest.getInsertOperationRequest().getRow().getVarLengthData().size(),
                                partialRow.getVarLengthData().size());
                    } catch (Exception e) {
                        LOG.error("Failed to read data ({})", threadNumber, e);
                        failed.set(true);
                    }
                    processed.incrementAndGet();
                });
            }
            try {
                GenericTestUtils.waitFor(() -> processed.get() == threads,
                        100, (int) TimeUnit.SECONDS.toMillis(5));
            } finally {
                executor.shutdownNow();
            }
            assertFalse(failed.get());
        } finally {
            deleteDir(tempPath.toFile());
        }
    }

    public void deleteDir(File file) {
        if (file.isDirectory()) {
            for (File f : file.listFiles())
                deleteDir(f);
        }
        file.delete();
    }

    @Test
    public void serialRead() throws Exception {
        PartialRow raw = ClientTestUtil.getPartialRowWithAllTypes();

        OperationRequest operation = OperationRequest.newBuilder()
                .setOperationType(OperationType.INSERT)
                .setInsertOperationRequest(new InsertOperationRequest(raw))
                .build();
        byte[] array = operation.toProto().toByteArray();
        ChunkBuffer data = ChunkBuffer.wrap(ByteBuffer.wrap(array));
        Path tempFile = Files.createTempDirectory(PREFIX);
        try {
            VolumeIOStats stats = new VolumeIOStats();
            long len = data.limit();
            long offset = 0;
            LStoreUtils.writeData(tempFile, data, offset, len, stats, true);

            OperationRequest operationRequest = OperationRequest.newBuilder()
                    .setOperationType(OperationType.SCAN_QUERY)
                    .setScanQueryOperationRequest(new ScanQueryOperationRequest("id = 1"))
                    .build();
            ByteBuffer[] readBuffers = BufferUtils.assignByteBuffers(len, len);
            LStoreUtils.readData(tempFile, readBuffers, ByteBuffer.wrap(operationRequest.toProto().toByteArray()), offset, len, stats);

            // There should be only one element in readBuffers
            Assert.assertEquals(1, readBuffers.length);
            ByteBuffer readBuffer = readBuffers[0];
            PartialRow partialRow = PartialRow.fromProtobuf(HetuPhotonProtos.PartialRowProto.parseFrom(readBuffer.array()));

            assertArrayEquals(raw.encodeColumnKey(), partialRow.encodeColumnKey());
            assertEquals(raw.getVarLengthData().size(),
                    partialRow.getVarLengthData().size());
            LOG.info("Read data: {}", partialRow);
        } catch (Exception e) {
            LOG.error("Failed to read data", e);
        } finally {
           deleteDir(tempFile.toFile());
        }
    }

    @Test
    public void concurrentProcessing() throws Exception {
        final int perThreadWait = 1000;
        final int maxTotalWait = 5000;
        int threads = 20;
        List<Path> paths = new LinkedList<>();

        try {
            ExecutorService executor = new ThreadPoolExecutor(threads, threads,
                    0, TimeUnit.SECONDS, new LinkedBlockingQueue<>());
            AtomicInteger processed = new AtomicInteger();
            for (int i = 0; i < threads; i++) {
                Path path = Files.createTempFile(PREFIX, String.valueOf(i));
                paths.add(path);
                executor.execute(() -> {
                    LStoreUtils.processFileExclusively(path, () -> {
                        try {
                            Thread.sleep(perThreadWait);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                        processed.incrementAndGet();
                        return null;
                    });
                });
            }
            try {
                GenericTestUtils.waitFor(() -> processed.get() == threads,
                        100, maxTotalWait);
            } finally {
                executor.shutdownNow();
            }
        } finally {
            for (Path path : paths) {
                FileUtils.deleteQuietly(path.toFile());
            }
        }
    }

    @Test
    public void readMissingFile() throws Exception {
        // given
        int len = 123;
        int offset = 0;
        File nonExistentFile = new File("nosuchfile");
        ByteBuffer[] bufs = BufferUtils.assignByteBuffers(len, len);
        VolumeIOStats stats = new VolumeIOStats();

        // when
        StorageContainerException e = LambdaTestUtils.intercept(
                StorageContainerException.class,
                () -> LStoreUtils.readData(nonExistentFile.toPath(), bufs, offset, len, stats));

        // then
        Assert.assertEquals(UNABLE_TO_FIND_CHUNK, e.getResult());
        deleteDir(nonExistentFile);
    }

}
