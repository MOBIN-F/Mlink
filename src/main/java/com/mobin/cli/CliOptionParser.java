package com.mobin.cli;

import com.mobin.FlinkException;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.flink.core.fs.Path;

import java.io.File;
import java.net.URL;

/**
 * CLI option parser for MLink client.
 */
public class CliOptionParser {

    public static final Option OPTION_SQL = Option
            .builder("sql")
            .longOpt("sql")
            .required(false)
            .numberOfArgs(1)
            .argName("flink sql file")
            .desc("flink sql file")
            .build();

    public static final Option OPTION_DDL = Option
            .builder("ddl")
            .longOpt("ddl")
            .required(false)
            .numberOfArgs(1)
            .argName("sql ddl")
            .desc("sql ddl")
            .build();

    public static final Option OPTION_DML = Option
            .builder("dml")
            .longOpt("dml")
            .required(false)
            .numberOfArgs(1)
            .argName("sql dml")
            .desc("sql dml")
            .build();

    private static final Options MLINK_CLIENT_OPTIONS = getMlinkClientOptions(new Options());

    public static Options getMlinkClientOptions(Options options){
        options.addOption(OPTION_SQL);
        options.addOption(OPTION_DDL);
        options.addOption(OPTION_DML);
        return options;
    }

    public static CliOptions parseClient(String[] args) {
        try {
            DefaultParser parser = new DefaultParser();
            CommandLine line = parser.parse(MLINK_CLIENT_OPTIONS, args, true);
            CliOptions cliOptions = new CliOptions(
                    checkUrl(line, CliOptionParser.OPTION_SQL),
                    checkUrl(line, CliOptionParser.OPTION_DDL),
                    checkUrl(line, CliOptionParser.OPTION_DML)
            );
            return cliOptions;
        } catch (ParseException e) {
            throw new FlinkException(e.getMessage());
        }
    }

    private static URL checkUrl(CommandLine line, Option option) {
        final URL url = checkUrls(line,option);
        if (url != null ) {
            return url;
        }
        return null;
    }

    private static URL checkUrls(CommandLine line, Option option) {
        if (line.hasOption(option.getOpt())) {
            final String url = line.getOptionValue(option.getOpt());
                   try {
                       return Path.fromLocalFile(new File(url).getAbsoluteFile())
                               .toUri()
                               .toURL();
                   } catch (Exception e) {
                       throw new FlinkException(
                               "Invalid path for option '"
                                       + option.getLongOpt()
                                       + "': "
                                       + url,
                               e);
                   }
        }
        return null;
    }

    public static void helpFormatter(){
        HelpFormatter helpFormatter = new HelpFormatter();
        helpFormatter.printHelp(" ", MLINK_CLIENT_OPTIONS);
    }
}
