package org.apache.hudi.delete;

import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.RemoteHoodieTableFileSystemView;
import org.apache.hudi.common.table.view.SyncableFileSystemView;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.delete.bean.HudiFileBean;
import org.apache.hudi.delete.util.DeleteUtils;
import org.apache.hudi.exception.HoodieException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.IOException;
import java.util.*;

public class DeleteView {

    private final static Logger log = LoggerFactory.getLogger(DeleteView.class);

    private final SyncableFileSystemView fsView;

    private final HoodieTableMetaClient metaClient;

    private final String timestamp;

    private final HoodieInstant hoodieInstant;

    public SyncableFileSystemView getFsView() {
        return fsView;
    }

    public HoodieTableMetaClient getMetaClient() {
        return metaClient;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public DeleteView(HoodieTableMetaClient metaClient, SyncableFileSystemView fsView, String timestamp) {
        this.metaClient = metaClient;
        this.fsView = fsView;
        this.timestamp = timestamp;

        // 获取本次的信息
        HoodieTimeline commitTimelineOpt = fsView.getTimeline();
        Optional<HoodieInstant> hoodieInstantOpt = commitTimelineOpt.getInstants()
                .filter(instant -> instant.isCompleted() && instant.getTimestamp()
                        .equals(timestamp)).findFirst();
        hoodieInstant = hoodieInstantOpt.get();
    }

    public DeleteView(HoodieTableMetaClient metaClient, RemoteHoodieTableFileSystemView fsView, HoodieInstant hoodieInstant) {
        this.metaClient = metaClient;
        this.fsView = fsView;
        this.hoodieInstant = hoodieInstant;
        this.timestamp = hoodieInstant.getTimestamp();
    }

    private final HashMap<String, HoodieCommitMetadata> cacheMetadata = new HashMap<>();

    public List<HudiFileBean> getDeleteFiles() {
        return getDeleteFiles(false);
    }

    /**
     * 获取基于当前时间坐标的标识为有删除数据的文件
     */
    public List<HudiFileBean> getDeleteFiles(boolean refresh) {
        List<HudiFileBean> deleteFiles = new ArrayList<>();

        if (refresh && fsView instanceof RemoteHoodieTableFileSystemView) {
            ((RemoteHoodieTableFileSystemView) fsView).refresh();
        }

        HoodieTimeline commitTimelineOpt = fsView.getTimeline();
        HoodieCommitMetadata commitMetadata;

        try {
            commitMetadata = HoodieCommitMetadata.fromBytes(
                    commitTimelineOpt.getInstantDetails(hoodieInstant).get(),
                    HoodieCommitMetadata.class);
            cacheMetadata.put(timestamp, commitMetadata);
        } catch (Exception e) {
            if (!refresh && fsView instanceof RemoteHoodieTableFileSystemView) {
                log.warn("there is something wrong in get HoodieCommitMetadata, maybe need to refresh remoteFsView");
                return getDeleteFiles(true);
            } else {
                throw new HoodieException(e.getMessage(), e);
            }
        }

        for (List<HoodieWriteStat> stats : commitMetadata.getPartitionToWriteStats().values()) {
            for (HoodieWriteStat stat : stats) {
                if (stat.getPrevCommit() != null && stat.getNumDeletes() > 0) {
                    deleteFiles.add(new HudiFileBean(timestamp, stat));
                }
            }
        }

        return deleteFiles;
    }

    /**
     * 基于删除数据获取前一个commit的文件列表
     */
    public List<HudiFileBean> getDeletePrevFiles(List<HudiFileBean> deleteFiles) {
        List<HudiFileBean> deletePrevFiles = new ArrayList<>();

        for (HudiFileBean deleteFile : deleteFiles) {
            String prevCommit = deleteFile.getHoodieWriteStat().getPrevCommit();

            HoodieCommitMetadata commitMetadata = getHcm(prevCommit);
            Optional<HoodieWriteStat> stat = findHoodieWriteStat(commitMetadata, deleteFile.getHoodieWriteStat().getFileId());

            if (stat.isPresent()) {
                deletePrevFiles.add(new HudiFileBean(prevCommit, stat.get()));
            } else {
                throw new HoodieException("不存在和 fileId = " + deleteFile.getHoodieWriteStat().getFileId() + " 对应的前置数据文件！\n" +
                        "HoodieWriteStat = " + deleteFile.getHoodieWriteStat());
            }
        }

        return deletePrevFiles;
    }

    private Optional<HoodieWriteStat> findHoodieWriteStat(HoodieCommitMetadata commitMetadata, String fileId) {
        for (Map.Entry<String, List<HoodieWriteStat>> entry : commitMetadata.getPartitionToWriteStats().entrySet()) {
            for (HoodieWriteStat stat : entry.getValue()) {
                if (stat.getFileId().equals(fileId)) {
                    return Optional.of(stat);
                }
            }
        }

        return Optional.empty();
    }

    /**
     * 获取prevcommit和commit对应关系的路径对照
     */
    public List<Tuple2<String, String>> getMapFileList(List<HudiFileBean> deletePrevFiles, List<HudiFileBean> deleteFiles) {
        ValidationUtils.checkArgument(deleteFiles.size() == deletePrevFiles.size(),
                "提交的两个列表的数组数量必须一致！");
        List<Tuple2<String, String>> mapList = new ArrayList<>();

        String basePath = metaClient.getBasePath();
        if (!basePath.endsWith("/")) basePath = basePath + "/";

        for (int i = 0 , rows = deleteFiles.size() ; i < rows ; i ++) {
            Tuple2<String, String> fileTuple = Tuple2.apply(
                    basePath + deletePrevFiles.get(i).getHoodieWriteStat().getPath(),
                    basePath + deleteFiles.get(i).getHoodieWriteStat().getPath());
            mapList.add(fileTuple);
        }

        return mapList;
    }

    /**
     * 获取删除数据对照文件的路径
     */
    public String getDeleteViewFilePath() {
        String basePath = metaClient.getBasePath();
        if (basePath.endsWith("/")) basePath = basePath.substring(0, basePath.length() - 1);
        return String.format("%s/.delete/%s/", basePath, timestamp);
    }

    /**
     * 校验下删除文件是否存在并且数据是否正确
     * @return true 存在，false 不存在
     */
    public boolean checkDeleteFileIsPresent() throws IOException {
        String deletePath = getDeleteViewFilePath();
        boolean containsFile = DeleteUtils.checkFileExists(deletePath)
                && DeleteUtils.getFileList(deletePath).length > 0;

        // 校验下数据是否一致
        if (containsFile) {
            long rows = DeleteUtils.getFileRowCount(deletePath);
            return rows == getHcm(timestamp).getTotalRecordsDeleted();
        }

        return false;
    }

    private HoodieCommitMetadata getHcm(String timestamp) {
        if (cacheMetadata.containsKey(timestamp)) {
            return cacheMetadata.get(timestamp);
        } else {
            HoodieTimeline commitTimelineOpt = fsView.getTimeline();

            // 获取本次的信息
            Optional<HoodieInstant> hoodieInstantOpt = commitTimelineOpt.getInstants()
                    .filter(instant -> instant.isCompleted() && instant.getTimestamp()
                            .equals(timestamp)).findFirst();

            ValidationUtils.checkArgument(hoodieInstantOpt.isPresent(),
                    "hudi instant can not be empty! timestamp = " + timestamp);
            HoodieCommitMetadata commitMetadata;
            try {
                commitMetadata = HoodieCommitMetadata.fromBytes(
                        commitTimelineOpt.getInstantDetails(hoodieInstantOpt.get()).get(),
                        HoodieCommitMetadata.class);
            } catch (Exception e) {
                throw new HoodieException(e.getMessage(), e);
            }

            cacheMetadata.put(timestamp, commitMetadata);
            return commitMetadata;
        }
    }
}
