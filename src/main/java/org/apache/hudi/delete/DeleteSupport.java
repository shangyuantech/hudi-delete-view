package org.apache.hudi.delete;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.AvroConversionUtils;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.view.RemoteHoodieTableFileSystemView;
import org.apache.hudi.delete.bean.HudiFileBean;
import org.apache.hudi.delete.util.DeleteUtils;
import org.apache.spark.SerializableWritable;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.io.IOException;
import java.util.List;

public class DeleteSupport {

    private String path;

    private String timestamp;

    private DeleteView deleteView;

    public static final String remoteHost = "localhost";
    public static final Integer remotePort = 26754;

    public DeleteSupport(String path, String timestamp) {
        this.path = path;
        this.timestamp = timestamp;
        initDeleteView();
    }

    private void initDeleteView() {
        HoodieTableMetaClient metaClient = new HoodieTableMetaClient(DeleteUtils.getConfiguration(),
                path, true);
        // 可以根据实际情况调整，我这里是做了单独的 timeline server 服务
        RemoteHoodieTableFileSystemView fsView = new RemoteHoodieTableFileSystemView(
                remoteHost, remotePort, metaClient);
        this.deleteView = new DeleteView(metaClient, fsView, timestamp);
    }

    public String getPath() {
        return path;
    }

    public String getTimestamp() {
        return timestamp;
    }

    /**
     * 读取数据并保存下来，下次就可以直接获取了
     */
    private void saveHudiDeleteFile() throws Exception {
        JavaSparkContext jsc = new JavaSparkContext(SparkSession.active().sparkContext());
        SerializableWritable<Configuration> serializedConf = new SerializableWritable<>(DeleteUtils.getConfiguration());

        List<HudiFileBean> deleteFiles = deleteView.getDeleteFiles();
        List<HudiFileBean> prevDeleteFiles = deleteView.getDeletePrevFiles(deleteFiles);
        List<Tuple2<String, String>> newFileList = deleteView.getMapFileList(prevDeleteFiles, deleteFiles);

        TableSchemaResolver schemaResolver = new TableSchemaResolver(deleteView.getMetaClient());


        JavaRDD<Tuple2<String, String>> rdd = jsc.parallelize(newFileList, newFileList.size());
        JavaRDD<GenericRecord> deleteRdd = rdd.flatMap(ft -> {
            // 处理筛选出删除数据
            HudiDeleteHandle deleteHandle = new HudiDeleteHandle(
                    new Path(ft._1), new Path(ft._2), timestamp, serializedConf.value()).init();
            return deleteHandle.filterDeleteRows().iterator();
        });

        Dataset<Row> df = AvroConversionUtils.createDataFrame(JavaRDD.toRDD(deleteRdd),
                schemaResolver.getTableAvroSchema().toString(),
                SparkSession.active());
        df.write().mode(SaveMode.Overwrite).parquet(deleteView.getDeleteViewFilePath());
    }

    /**
     * 获取删除数据集
     */
    public Dataset<Row> getDeleteDataset() throws Exception {
        if (!deleteView.checkDeleteFileIsPresent()) {// 如果存在，则直接拿来计算
            saveHudiDeleteFile();
        }

        SparkSession spark = SparkSession.active();
        return spark.read().parquet(deleteView.getDeleteViewFilePath());
    }
}
