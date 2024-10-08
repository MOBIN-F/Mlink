package com.mobin;

import com.mobin.cli.CliOptionParser;
import com.mobin.cli.CliOptions;
import com.mobin.cli.CliStatementSplitter;
import org.apache.commons.io.IOUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.internal.TableEnvironmentInternal;
import org.apache.flink.table.delegation.Parser;
import org.apache.flink.table.operations.ExplainOperation;
import org.apache.flink.table.operations.ModifyOperation;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.SinkModifyOperation;
import org.apache.flink.table.operations.command.SetOperation;
import org.apache.flink.table.operations.ddl.CreateTableOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;


/**
 * FlinkClient is the main class of the Flink SQL Client.
 */
public class FlinkClient {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkClient.class);

    private CliOptions options;

    public FlinkClient() {}

    public FlinkClient(CliOptions options) {
        this.options = options;
    }

    public static void main(String[] args) {
        if (args.length == 0) {
            CliOptionParser.helpFormatter();
        }
        final CliOptions options = CliOptionParser.parseClient(args);
        final FlinkClient client = new FlinkClient(options);
        client.start();
    }

    public void start() {
        if (options.getSqlFile() != null) {
            executeInitialization(readFromURL(options.getSqlFile()));
        } else if (options.getDdlFile() != null && options.getDmlFile() != null) {
            executeInitialization(readFromURLs(options.getDdlFile(), options.getDmlFile()));
        }
    }

    private String readFromURL(URL file) {
        try {
            return IOUtils.toString(file, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new FlinkException(
                    String.format("Fail to read content from the %s.",
                            file.getPath()), e);
        }
    }

    private String readFromURLs(URL ddlFile, URL dmlFile) {
        try {
            String sql = IOUtils.toString(ddlFile, StandardCharsets.UTF_8)
                    .concat(System.getProperty("line.separator"))
                    .concat(IOUtils.toString(dmlFile, StandardCharsets.UTF_8));
            return sql;
        } catch (IOException e) {
            throw new FlinkException(
                    String.format("Fail to read content from the %s , %s.",
                            dmlFile.getPath() , dmlFile.getPath()), e);
        }
    }

    private void setDefaultSettings(TableEnvironment tableEnv, boolean isStreaming){
        Configuration tableConf = tableEnv.getConfig().getConfiguration();
        tableEnv.getConfig().setLocalTimeZone(ZoneOffset.ofHours(8));
        if (isStreaming) {
            tableConf.setString("state.backend.incremental", "true");
            tableConf.setString("execution.checkpointing.interval", "2min");
            tableConf.setString("execution.checkpointing.min-pause", "10s");
            tableConf.setString("execution.checkpointing.externalized-checkpoint-retention", "RETAIN_ON_CANCELLATION");
        }

        tableConf.setBoolean("table.dynamic-table-options.enabled",true);
        tableConf.setString("restart-strategy", "failure-rate");
        tableConf.setString("restart-strategy.failure-rate.delay", "10s");
        tableConf.setString("restart-strategy.failure-rate.failure-rate-interval", "5min");
        tableConf.setInteger("restart-strategy.failure-rate.max-failures-per-interval", 3);
    }

    public TableResult executeInitialization(String content) {
        return executeStatement(content);
    }

    private TableResult executeStatement(String content) {
        Tuple2<List<String>, Boolean> statementsAndMode = CliStatementSplitter.splitContent(content);
        EnvironmentSettings settings = statementsAndMode.f1
                ?
                EnvironmentSettings.newInstance()
                        .inStreamingMode()
                        .build()
                :
                EnvironmentSettings.newInstance()
                        .inBatchMode()
                        .build();
        if (settings.isStreamingMode()) {
            LOG.info("------------------Flink Streaming Job------------------");
        } else {
            LOG.info("------------------Flink Batch Job------------------");
        }
        TableEnvironment tableEnv = TableEnvironment.create(settings);
        setDefaultSettings(tableEnv, statementsAndMode.f1);
        TableEnvironmentInternal tabEnvInternal = (TableEnvironmentInternal) tableEnv;


        List<ModifyOperation> InsertOperations = new ArrayList<>();
        Parser parser = tabEnvInternal.getParser();

        LOG.info("------------------Flink SQL Execution------------------");
        for (String statement : statementsAndMode.f0) {
            try {
                Operation opt = parser.parse(statement).get(0);
                LOG.info(statement);
                if (opt instanceof SinkModifyOperation) {
                    //INSERT
                    InsertOperations.add((ModifyOperation) opt);
                } else if (opt instanceof SetOperation) {
                    //SET
                    callSet((SetOperation) opt, tabEnvInternal);
                } else if (opt instanceof ExplainOperation) {
                    //EXPLAIN
                    callExplain((ExplainOperation) opt, tabEnvInternal, statement);
                } else if (opt instanceof CreateTableOperation) {
                    //CREATE TABLE
                    callCreateTable((CreateTableOperation) opt, tabEnvInternal);
                } else {
                    executOperation(opt, tabEnvInternal);
                }
            } catch (Exception e) {
                errorSQLOut(statement);
                e.printStackTrace(System.out);
                System.exit(-1);
            }
        }

        if (!InsertOperations.isEmpty()) {
            return callInsert(InsertOperations, tabEnvInternal);
        }
        return null;
    }

    private TableResult callInsert(List<ModifyOperation> InsertOperations, TableEnvironmentInternal tabEnvInternal) {
        TableResult tableResult = tabEnvInternal.executeInternal(InsertOperations);
        return tableResult;
    }

    private void callCreateTable(CreateTableOperation operation, TableEnvironmentInternal tabEnvInternal) {
        tabEnvInternal.executeInternal(operation);
    }

    private void executOperation(Operation operation, TableEnvironmentInternal tabEnvInternal) {
        tabEnvInternal.executeInternal(operation);
    }

    private void callExplain(ExplainOperation operation, TableEnvironmentInternal tabEnvInternal, String statement) {
        TableResult tableResult = tabEnvInternal.executeInternal(operation);
        final String explaination =
                Objects.requireNonNull(tableResult.collect().next().getField(0)).toString();
        LOG.info("------------------SQL Explain：\n" ,statement);
        LOG.info(explaination);
        System.exit(0);
    }

    private void callSet(SetOperation setOperation, TableEnvironmentInternal tabEnvInternal) {
        if (setOperation.getKey().isPresent() && setOperation.getValue().isPresent()) {
            String key = setOperation.getKey().get().trim();
            String value = setOperation.getValue().get().trim();
            tabEnvInternal.getConfig().getConfiguration().setString(key, value);
        }
    }

    private void errorSQLOut(String msg) {
        LOG.error("------------------Error SQL：");
        String[] splitStr = msg.split("\n");
        for (int i = 0,lineNum=1; i < splitStr.length; i ++, lineNum ++) {
            System.out.println(lineNum + ": " + splitStr[i]);
        }
    }
}
