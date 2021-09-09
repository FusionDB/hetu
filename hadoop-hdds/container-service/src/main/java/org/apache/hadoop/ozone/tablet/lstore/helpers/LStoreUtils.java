package org.apache.hadoop.ozone.tablet.lstore.helpers;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.hetu.photon.operation.OperationType;
import org.apache.hadoop.hetu.photon.helpers.PartialRow;
import org.apache.hadoop.hetu.photon.meta.table.ColumnSchema;
import org.apache.hadoop.hetu.photon.meta.table.Schema;
import org.apache.hadoop.hetu.photon.operation.request.InsertOperationRequest;
import org.apache.hadoop.hetu.photon.operation.request.OperationRequest;
import org.apache.hadoop.ozone.common.ChunkBuffer;
import org.apache.hadoop.ozone.container.common.volume.VolumeIOStats;
import org.apache.hadoop.util.Time;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.BinaryPoint;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.CONTAINER_INTERNAL_ERROR;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.INVALID_WRITE_SIZE;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.IO_EXCEPTION;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.NO_SUCH_ALGORITHM;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.UNABLE_TO_FIND_CHUNK;

/**
 * Created by xiliu on 2021/5/21
 */
public class LStoreUtils {
    private static final Set<Path> LOCKS = ConcurrentHashMap.newKeySet();
    private static long OPERATION_DEFAULT_SIZE = 10;
    private static final Logger LOG =
            LoggerFactory.getLogger(LStoreUtils.class);

    public LStoreUtils() {
    }

    public static void writeData(Path path, ChunkBuffer data,
                                 long offset, long len, VolumeIOStats volumeIOStats, boolean sync) throws StorageContainerException {
        validateBufferSize(len, (data.limit() - OPERATION_DEFAULT_SIZE));
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
                try {
                    OperationRequest operation = OperationRequest.fromPersistedFormat(byteBuffer.array());
                    processFileExclusively(path, () -> {
                        try {
                            if (operation.getOperationType().equals(OperationType.INSERT)) {
                                InsertOperationRequest insertOperation = operation.getInsertOperationRequest();
                                int length = insertRow(indexWriter, insertOperation.getRow());
                                bytesWritten.getAndAdd(length);
                            } else if (operation.getOperationType().equals(OperationType.UPDATE)) {
                                // TODO
                            } else if (operation.getOperationType().equals(OperationType.DELETE)) {
                                // TODO
                            } else if (operation.getOperationType().equals(OperationType.UPSERT)) {
                                // TODO
                            } else {
                                throw new RuntimeException("Unsupported operation type of write: " + operation.getOperationType());
                            }
                            return byteBuffer.array().length;
                        } catch (IOException e) {
                            LOG.error(e.getMessage(), e);
                        }
                        return null;
                    });
                } catch (IOException e) {
                    LOG.error(e.getMessage(), e);
                }
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

        validateWriteSize(len, bytesWritten.get());
    }

    private static int insertRow(IndexWriter indexWriter, PartialRow row) throws IOException {
        Schema schema = row.getSchema();
        List<ColumnSchema> columnSchemas = schema.getColumns();
        Document doc = new Document();
        columnSchemas.stream().forEach(columnSchema -> {
            BytesRef bytesRef;
            switch (columnSchema.getColumnType()) {
                case BOOL:
                    FieldType fieldType = new FieldType();
                    fieldType.setStored(true);
                    fieldType.setStoreTermVectors(false);
                    fieldType.setStoreTermVectorOffsets(false);
                    fieldType.setStoreTermVectorPositions(false);
//                    fieldType.setDocValuesType(DocValuesType.SORTED);
                    fieldType.setTokenized(false);
                    fieldType.setIndexOptions(IndexOptions.DOCS);
                    bytesRef = new BytesRef(String.valueOf(row.getBoolean(columnSchema.getColumnName())));
                    Field field = new Field(columnSchema.getColumnName(), bytesRef, fieldType);
                    doc.add(field);
                    break;
                case INT8:
                    bytesRef = new BytesRef(String.valueOf(row.getByte(columnSchema.getColumnName())));
                    doc.add(new StringField(columnSchema.getColumnName(), bytesRef, Field.Store.YES));
                    break;
                case INT16:
                    doc.add(new IntPoint(columnSchema.getColumnName(), row.getShort(columnSchema.getColumnName())));
                    break;
                case INT32:
                    doc.add(new IntPoint(columnSchema.getColumnName(), row.getInt(columnSchema.getColumnName())));
                    break;
                case INT64:
                    doc.add(new LongPoint(columnSchema.getColumnName(), row.getLong(columnSchema.getColumnName())));
                    break;
                case DOUBLE:
                    doc.add(new DoublePoint(columnSchema.getColumnName(), row.getDouble(columnSchema.getColumnName())));
                    break;
                case FLOAT:
                    doc.add(new FloatPoint(columnSchema.getColumnName(), row.getFloat(columnSchema.getColumnName())));
                    break;
                case STRING:
                    doc.add(new TextField(columnSchema.getColumnName(), row.getString(columnSchema.getColumnName()), Field.Store.YES));
                    break;
                case DATE:
                    doc.add(new LongPoint(columnSchema.getColumnName(), row.getDate(columnSchema.getColumnName()).getTime()));
                    break;
                case DECIMAL:
                    bytesRef = new BytesRef(row.getDecimal(columnSchema.getColumnName()).toString());
                    doc.add(new StringField(columnSchema.getColumnName(), bytesRef, Field.Store.YES));
                    break;
                case BINARY:
                    bytesRef = new BytesRef(row.getBinaryCopy(columnSchema.getColumnName()));
                    doc.add(new BinaryPoint(columnSchema.getColumnName(), bytesRef.bytes));
                    break;
                case VARCHAR:
                    doc.add(new TextField(columnSchema.getColumnName(), row.getVarchar(columnSchema.getColumnName()), Field.Store.YES));
                    break;
                case UNIXTIME_MICROS:
                    doc.add(new LongPoint(columnSchema.getColumnName(), row.getTimestamp(columnSchema.getColumnName()).getTime()));
                    break;
                default:
                    throw new RuntimeException("Unsupported data type: " + columnSchema.getColumnType()
                            + " of column name: " + columnSchema.getColumnName());
            }
        });

        // add _source
        BytesRef source = new BytesRef(row.toProtobuf().toByteArray());
        doc.add(new StoredField("_source", source));
        doc.add(new SortedDocValuesField("_source", source));
        doc.add(new StoredField("_id", row.encodeColumnKey()));
        indexWriter.addDocument(doc);
        return source.length;
    }

    public static void validateBufferSize(long expected, long actual)
            throws StorageContainerException {
        checkSize("buffer", expected, actual, INVALID_WRITE_SIZE);
    }

    private static void validateWriteSize(long expected, long actual)
            throws StorageContainerException {
        checkSize("write", expected, actual, INVALID_WRITE_SIZE);
    }

    private static void checkSize(String of, long expected, long actual,
                                  ContainerProtos.Result code) throws StorageContainerException {
        if (actual != expected) {
            String err = String.format(
                    "Unexpected %s size. expected: %d, actual: %d",
                    of, expected, actual);
            LOG.error(err);
            throw new StorageContainerException(err, code);
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


    public static void readData(Path path, ByteBuffer[] buffers, ByteBuffer scanQueryOperation,
                                long offset, long len, VolumeIOStats volumeIOStats)
            throws StorageContainerException {
        final long startTime = Time.monotonicNow();
        long bytesRead = 0;
        IndexReader reader = null;
        try {
            reader = DirectoryReader.open(FSDirectory.open(path));
            IndexSearcher indexSearcher = new IndexSearcher(reader);

            final Query q1 =
                    new BooleanQuery.Builder()
                            .add(new TermQuery(new Term("bool", "true")), BooleanClause.Occur.MUST)
                    .build();
            TopDocs results = indexSearcher.search(q1, 10);
            ScoreDoc[] hits = results.scoreDocs;
            LOG.debug("Found " + hits.length + " hits.");
            for (int i = 0; i < hits.length; i++) {
                int docId = hits[i].doc;
                Document d = indexSearcher.doc(docId);
                BytesRef rowBytes = d.getBinaryValue("_source");
                bytesRead += rowBytes.length;
                buffers[i] = ByteBuffer.wrap(rowBytes.bytes);
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

    public static void readData(Path path, ByteBuffer[] buffers,
                                long offset, long len, VolumeIOStats volumeIOStats)
            throws StorageContainerException {
        final long startTime = Time.monotonicNow();
        long bytesRead = 0;
        IndexReader reader = null;
        try {
            reader = DirectoryReader.open(FSDirectory.open(path));
            // Read DocValue
            List<LeafReaderContext> subReaders = reader.leaves();
            for (int i = 0; i < subReaders.size(); i++) {
                LeafReaderContext subReader = subReaders.get(i);
                LeafReader leafReader = subReader.reader();
                BinaryDocValues docValues = DocValues.getBinary(leafReader, "_source");
                ByteBuffer rowOpBytes = ByteBuffer.wrap(docValues.binaryValue().bytes);
                bytesRead += rowOpBytes.limit();
                buffers[i] = rowOpBytes;
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
