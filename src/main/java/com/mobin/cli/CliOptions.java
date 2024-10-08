package com.mobin.cli;

import java.net.URL;

/**
 * Class to hold the command line options
 */
public class CliOptions {
    private final URL sqlFile;
    private final URL ddlFile;
    private final URL dmlFile;

    public CliOptions(URL sqlFile, URL ddlFile, URL dmlFile){
        this.sqlFile = sqlFile;
        this.ddlFile = ddlFile;
        this.dmlFile = dmlFile;
    }

    public URL getSqlFile() {
        return sqlFile;
    }

    public URL getDdlFile() {
        return ddlFile;
    }

    public URL getDmlFile() {
        return dmlFile;
    }

}
