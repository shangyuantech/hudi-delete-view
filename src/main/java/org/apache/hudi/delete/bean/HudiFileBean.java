package org.apache.hudi.delete.bean;

import org.apache.hudi.common.model.HoodieWriteStat;

public class HudiFileBean {

    public HudiFileBean(String timestamp, HoodieWriteStat hoodieWriteStat) {
        this.timestamp = timestamp;
        this.hoodieWriteStat = hoodieWriteStat;
    }

    private String timestamp;

    private HoodieWriteStat hoodieWriteStat;

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public HoodieWriteStat getHoodieWriteStat() {
        return hoodieWriteStat;
    }

    public void setHoodieWriteStat(HoodieWriteStat hoodieWriteStat) {
        this.hoodieWriteStat = hoodieWriteStat;
    }

    @Override
    public String toString() {
        return "HudiFileBean{" +
                "timestamp='" + timestamp + '\'' +
                ", hoodieWriteStat=" + hoodieWriteStat +
                '}';
    }
}

