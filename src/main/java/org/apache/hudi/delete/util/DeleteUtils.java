package org.apache.hudi.delete.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.parquet.hadoop.Footer;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.spark.sql.SparkSession;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class DeleteUtils {

    private static final Logger log = LoggerFactory.getLogger(DeleteUtils.class);

    public static Configuration getConfiguration() {
        return SparkSession.active().sparkContext().hadoopConfiguration();
    }

    public static long getFileRowCount(String path) throws IOException {

        Path inputPath = new Path(path);
        FileStatus[] inputFileStatuses = inputPath.getFileSystem(getConfiguration())
                .globStatus(inputPath);

        long totalRowCount = 0L;
        for (FileStatus fs : inputFileStatuses) {
            long fileRowCount = 0L;
            for (Footer f : ParquetFileReader.readFooters(getConfiguration(), fs, false)) {
                long blockRowCount = 0L;
                for (BlockMetaData b : f.getParquetMetadata().getBlocks()) {
                    fileRowCount += b.getRowCount();
                    blockRowCount += b.getRowCount();
                }
                log.info("{} row count: {}", f.getFile().getName(), blockRowCount);
            }
            totalRowCount += fileRowCount;
        }

        return totalRowCount;
    }

    public static boolean checkFileExists(String path) throws IOException {
        FileSystem fs = FSUtils.getFs(path, getConfiguration());
        return fs.exists(new Path(path));
    }

    public static Path[] getFileList(String path) throws IOException {
        FileSystem fs = FSUtils.getFs(path, getConfiguration());
        FileStatus[] fss = fs.listStatus(new Path(path));
        return FileUtil.stat2Paths(fss);
    }
}
