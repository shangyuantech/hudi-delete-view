package org.apache.hudi.delete;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.io.storage.HoodieFileReader;
import org.apache.hudi.io.storage.HoodieFileReaderFactory;

import java.io.IOException;
import java.io.Serializable;
import java.text.ParseException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class HudiDeleteHandle implements Serializable {

    private final Set<String> recordKeys = new HashSet<>();

    private final Path newFilePath;

    private final Path oldFilePath;

    private final Configuration configuration;

    private final String timestamp;

    private HoodieFileReader<GenericRecord> oldFileReader;

    public Set<String> getRecordKeys() {
        return recordKeys;
    }

    public HudiDeleteHandle(Path oldFilePath, Path newFilePath, String timestamp, Configuration configuration) {
        this.newFilePath = newFilePath;
        this.oldFilePath = oldFilePath;
        this.configuration = configuration;
        this.timestamp = timestamp;
    }

    public HudiDeleteHandle init() throws IOException {
        HoodieFileReader<GenericRecord> newFileReader = HoodieFileReaderFactory.getFileReader(configuration, newFilePath);
        Schema readSchema = newFileReader.getSchema();

        final Iterator<GenericRecord> readerIterator = newFileReader.getRecordIterator(readSchema);
        while (readerIterator.hasNext()) {
            GenericRecord record = readerIterator.next();
            recordKeys.add(String.valueOf(record.get(HoodieRecord.RECORD_KEY_METADATA_FIELD)));
        }

        newFileReader.close();
        return this;
    }

    /**
     * 过滤删除的数据
     */
    public List<GenericRecord> filterDeleteRows() throws IOException {
        try {
            oldFileReader = HoodieFileReaderFactory.getFileReader(configuration, oldFilePath);
            Schema readSchema = oldFileReader.getSchema();
            Iterator<GenericRecord> readerIterator = oldFileReader.getRecordIterator(readSchema);

            Stream<GenericRecord> stream = StreamSupport.stream(
                    Spliterators.spliteratorUnknownSize(readerIterator,Spliterator.ORDERED)
                    , false);
            return stream
                    .filter(record -> !recordKeys.contains(
                            String.valueOf(record.get(HoodieRecord.RECORD_KEY_METADATA_FIELD))))
                    // 这里认为把提交日期修改为处理日期可能更好一些
                    .peek(record -> record.put(HoodieRecord.COMMIT_TIME_METADATA_FIELD, timestamp))
                    .collect(Collectors.toList());
        } finally {
            if (oldFileReader != null) {
                oldFileReader.close();
            }
        }
    }
}
