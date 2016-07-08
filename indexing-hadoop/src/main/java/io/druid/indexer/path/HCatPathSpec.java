package io.druid.indexer.path;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.metamx.common.logger.Logger;
import io.druid.indexer.HadoopDruidIndexerConfig;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;

import java.io.IOException;

public class HCatPathSpec implements PathSpec {
    private static final Logger log = new Logger(HCatPathSpec.class);

    private final String databaseName;
    private final String tableName;
    private final String filter;

    @JsonCreator
    public HCatPathSpec(
        @JsonProperty("database") String databaseName,
        @JsonProperty("table") String tableName,
        @JsonProperty("filter") String filter
    ) {
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.filter = filter;
    }

    @Override
    public Job addInputPaths(HadoopDruidIndexerConfig config, Job job) throws IOException {
        log.info("Adding input path: database=" + databaseName + ", table=" + tableName + ", filter=" + filter);
        HCatInputFormat.setInput(job, databaseName, tableName, filter);
        return job;
    }

    @JsonProperty
    public Class<? extends InputFormat> getInputFormat() {
        return HCatInputFormat.class;
    }
}
