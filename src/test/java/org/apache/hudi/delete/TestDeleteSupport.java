package org.apache.hudi.delete;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Test;

public class TestDeleteSupport {

    @Test
    public void test() throws Exception {
        String hudiPath = "/hive/warehouse/test.db/test/";
        String timstamp = "202012121212";

        DeleteSupport deleteSupport = new DeleteSupport(hudiPath, timstamp);
        Dataset<Row> deleteRows = deleteSupport.getDeleteDataset();
        deleteRows.show();
    }
}
