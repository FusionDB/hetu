package org.apache.hadoop.ozone.tablet.lstore.helpers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.json.JsonReadFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.hetu.Row;
import org.apache.hadoop.hetu.encoder.ByteBufferUtil;
import org.apache.hadoop.hetu.types.StructField;
import org.apache.hadoop.hetu.types.StructType;
import org.apache.hadoop.hetu.util.StandardTypes;
import org.apache.hadoop.ozone.common.ChunkBuffer;
import org.apache.hadoop.ozone.container.common.volume.VolumeIOStats;
import org.apache.hadoop.util.Time;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;
import org.eclipse.jetty.util.ajax.JSON;
import org.rocksdb.util.ByteUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.CONTAINER_INTERNAL_ERROR;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.IO_EXCEPTION;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.NO_SUCH_ALGORITHM;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.UNABLE_TO_FIND_CHUNK;

/**
 * Created by xiliu on 2021/5/21
 */
public class LStoreUtils {
    private static final Set<Path> LOCKS = ConcurrentHashMap.newKeySet();
    private static final ObjectMapper mapper = JsonMapper.builder()
            .enable(JsonReadFeature.ALLOW_BACKSLASH_ESCAPING_ANY_CHARACTER)
            .build();

    private static final Logger LOG =
            LoggerFactory.getLogger(LStoreUtils.class);

    public LStoreUtils() {
    }

    public static void writeData(Path path, ChunkBuffer data,
                                 long offset, long len, VolumeIOStats volumeIOStats, boolean sync) {
        AtomicLong bytesWritten = new AtomicLong();
        final long startTime = Time.monotonicNow();
        Analyzer analyzer = new StandardAnalyzer();
        Directory dir = null;
        try {
            dir = FSDirectory.open(path);
            IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
            iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
            IndexWriter indexWriter = new IndexWriter(dir, iwc);
            List<ByteBuffer> byteBufferList = data.asByteBufferList();
            byteBufferList.stream().forEach(byteBuffer -> {
//                write(indexWriter, byteBuffer);
                String row = new String(byteBuffer.array(), Charset.forName("UTF-8"));
                try {
                    Map<String, Object> treeMap = mapper.readValue(row, Map.class);
                    processFileExclusively(path, () -> {
                        treeMap.keySet().stream().forEach(key -> {
                            Document doc = new Document();
                            BytesRef bytesRef = new BytesRef(treeMap.get(key).toString());
                            doc.add(new SortedDocValuesField(key, bytesRef));
                            try {
                                indexWriter.addDocument(doc);
                            } catch (IOException e) {
                                LOG.error(e.getMessage(), e);
                            }
                        });
                        return row.getBytes().length;
                    });
                } catch (JsonProcessingException e) {
                    LOG.error(e.getMessage(), e);
                }
                bytesWritten.getAndAdd(byteBuffer.limit());
            });
            if (sync) {
                indexWriter.commit();
            }
            indexWriter.close();
        } catch (IOException e) {
            LOG.error(e.getMessage(), e);
        }
        final long endTime = Time.monotonicNow();
        long elapsed = endTime - startTime;
        offset = bytesWritten.get();
        len = bytesWritten.get();
        volumeIOStats.incWriteTime(elapsed);
        volumeIOStats.incWriteOpCount();
        volumeIOStats.incWriteBytes(bytesWritten.get());

        LOG.debug("Written {} bytes at offset {} to {} in {} ms",
                bytesWritten, offset, path, elapsed);
    }

    private static void write(IndexWriter indexWriter, ByteBuffer byteBuffer) {
        try {
            Row row = (Row) ByteBufferUtil.getObject(byteBuffer.array());
            StructType structType = row.schema;
            Map<String, Integer> nameToIndex = structType.nameToIndex();
            nameToIndex.keySet().forEach(name -> {
                Document doc = new Document();
                BytesRef bytesRef = new BytesRef(row.get(nameToIndex.get(name)).toString());
                StructField structField = structType.getStructField(name);
                if (structField.getDataType().getDisplayName().equalsIgnoreCase(StandardTypes.BIGINT)) {
                    doc.add(new SortedDocValuesField(name, bytesRef));
                } else if (structField.getDataType().getDisplayName().equalsIgnoreCase(StandardTypes.BOOLEAN)) {
                    doc.add(new SortedDocValuesField(name, bytesRef));
                } else if (structField.getDataType().getDisplayName().equalsIgnoreCase(StandardTypes.CHAR)) {
                    doc.add(new SortedDocValuesField(name, bytesRef));
                } else if (structField.getDataType().getDisplayName().equalsIgnoreCase(StandardTypes.DATE)) {
                    doc.add(new SortedDocValuesField(name, bytesRef));
                } else if (structField.getDataType().getDisplayName().equalsIgnoreCase(StandardTypes.DECIMAL)) {
                    doc.add(new SortedDocValuesField(name, bytesRef));
                } else if (structField.getDataType().getDisplayName().equalsIgnoreCase(StandardTypes.GEOMETRY)) {
                    doc.add(new SortedDocValuesField(name, bytesRef));
                } else if (structField.getDataType().getDisplayName().equalsIgnoreCase(StandardTypes.HYPER_LOG_LOG)) {
                    doc.add(new SortedDocValuesField(name, bytesRef));
                } else if (structField.getDataType().getDisplayName().equalsIgnoreCase(StandardTypes.ARRAY)) {
                    doc.add(new SortedDocValuesField(name, bytesRef));
                } else if (structField.getDataType().getDisplayName().equalsIgnoreCase(StandardTypes.DOUBLE)) {
                    doc.add(new SortedDocValuesField(name, bytesRef));
                } else if (structField.getDataType().getDisplayName().equalsIgnoreCase(StandardTypes.INTEGER)) {

                } else if (structField.getDataType().getDisplayName().equalsIgnoreCase(StandardTypes.MAP)) {

                } else if (structField.getDataType().getDisplayName().equalsIgnoreCase(StandardTypes.SMALLINT)) {

                } else if (structField.getDataType().getDisplayName().equalsIgnoreCase(StandardTypes.REAL)) {

                } else if (structField.getDataType().getDisplayName().equalsIgnoreCase(StandardTypes.TIME)) {

                } else if (structField.getDataType().getDisplayName().equalsIgnoreCase(StandardTypes.TIMESTAMP)) {

                } else if (structField.getDataType().getDisplayName().equalsIgnoreCase(StandardTypes.TINYINT)) {

                } else if (structField.getDataType().getDisplayName().equalsIgnoreCase(StandardTypes.VARBINARY)) {

                } else if (structField.getDataType().getDisplayName().equalsIgnoreCase(StandardTypes.VARCHAR)) {

                } else {
                    throw new RuntimeException("Unsupported data type");
                }
                doc.add(new SortedDocValuesField(name, bytesRef));
                try {
                    indexWriter.addDocument(doc);
                } catch (IOException e) {
                    LOG.error(e.getMessage(), e);
                }
            });
        } catch (IOException e) {
            LOG.error(e.getMessage(), e);
        } catch (ClassNotFoundException e) {
            LOG.error(e.getLocalizedMessage(), e);
        }
    }

    @VisibleForTesting
    public static <T> T processFileExclusively(Path path, Supplier<T> op) {
        for (; ; ) {
            if (LOCKS.add(path)) {
                break;
            }
        }

        try {
            return op.get();
        } finally {
            LOCKS.remove(path);
        }
    }

    public static void readData(Path path, ByteBuffer[] buffers,
                                long offset, long len, VolumeIOStats volumeIOStats)
            throws StorageContainerException {
        final long startTime = Time.monotonicNow();
        long bytesRead = 0;
        IndexReader reader = null;
        try {
            reader = DirectoryReader.open(FSDirectory.open(path));
            List<LeafReaderContext> subReaders = reader.leaves();
            for (int i = 0; i < subReaders.size(); i++) {
                LeafReaderContext subReader = subReaders.get(i);
                Map<String, Object> data = new HashMap<>();
                LeafReader leafReader = subReader.reader();
                FieldInfos fieldInfos = leafReader.getFieldInfos();
                fieldInfos.forEach(fieldInfo -> {
                    try {
                        data.put(fieldInfo.name, leafReader.getSortedDocValues(fieldInfo.name).binaryValue().utf8ToString());
                    } catch (IOException e) {
                        LOG.error(e.getMessage(), e);
                    }
                });
                ByteBuffer row = ByteBuffer.wrap(JSON.toString(data).getBytes());
                bytesRead += row.limit();
                buffers[i] = row;
            }
            // Increment volumeIO stats here.
            long endTime = Time.monotonicNow();
            volumeIOStats.incReadTime(endTime - startTime);
            volumeIOStats.incReadOpCount();
            offset = bytesRead;
            volumeIOStats.incReadBytes(bytesRead);

            LOG.debug("Read {} bytes starting at offset {} from {}",
                    bytesRead, offset, path);
        } catch (IOException e) {
            LOG.error(e.getMessage(), e);
            throw wrapInStorageContainerException(e);
        } finally {
            try {
                if (reader != null) {
                    reader.close();
                }
            } catch (IOException e) {
                LOG.error(e.getMessage(), e);
            }
        }
    }

    private static StorageContainerException wrapInStorageContainerException(
            IOException e) {
        ContainerProtos.Result result = translate(e);
        return new StorageContainerException(e, result);
    }

    private static ContainerProtos.Result translate(Exception cause) {
        if (cause instanceof FileNotFoundException ||
                cause instanceof NoSuchFileException) {
            return UNABLE_TO_FIND_CHUNK;
        }

        if (cause instanceof IOException) {
            return IO_EXCEPTION;
        }

        if (cause instanceof NoSuchAlgorithmException) {
            return NO_SUCH_ALGORITHM;
        }

        return CONTAINER_INTERNAL_ERROR;
    }
}
