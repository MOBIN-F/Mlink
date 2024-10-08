package com.mobin.cli;

import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * This class is used to split the input SQL statements into multiple statements.
 */
public class CliStatementSplitter {

    private static final String MASK = "--.*$";
    private static final String SET_MSCK = "(SET|set)(\\s+[']?(execution.runtime-mode)[']?\\s*=?\\s*[']?(BATCH|batch)[']?\\s*)?";
    private static Boolean flag = true;

    /**
     *
     * @param content
     * @return statements,isStreaming
     */
    public static Tuple2<List<String>, Boolean> splitContent(String content) {
        List<String> statements = new ArrayList<>();
        List<String> buffer = new ArrayList<>();
        boolean isStreaming = true;
        for (String line : content.split("\n")) {
            line = line.trim();
            if (isEndOfStatement(line)) {
                String tmp = line.substring(0, line.indexOf(";")).trim();
                if (flag) {
                    isStreaming = isSetFlinkRuntimeMode(tmp);
                }
                buffer.add(tmp);
                statements.add(String.join("\n", buffer));
                buffer.clear();
            } else {
                if (!line.replaceAll(MASK, "").isEmpty()) {
                    buffer.add(line);
                }
            }
        }
        return new Tuple2<>(statements, isStreaming);
    }


    private static boolean isEndOfStatement(String line) {
        return line.replaceAll(MASK,"").trim().endsWith(";");
    }

    private static boolean isSetFlinkRuntimeMode(String line) {
        if (Pattern.matches(SET_MSCK, line)) {
            flag = false;
            return false;
        }
        return true;
    }
}
