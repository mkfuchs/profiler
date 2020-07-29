package com.mkf.profiler;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.sqlite.SQLiteConfig;

class Main {
    private static final Options cliOptions = new Options();

    public static void main(String[] args) throws Exception {

        System.setProperty("org.graphstream.ui.renderer",
                "org.graphstream.ui.j2dviewer.J2DGraphRenderer");
        CsvProfiler profiler = new CsvProfiler();

        CommandLine cmd = processCliArgs(args, profiler);

        SQLiteConfig config = new SQLiteConfig();
        config.enforceForeignKeys(true);
        Connection connection = DriverManager.getConnection(
                String.format("jdbc:sqlite:%s", profiler.databaseFilePath), config.toProperties());
        connection.setAutoCommit(false);

        Map<String, String> tablenameFilepathMap = new HashMap<>();
        ArrayList<String> columns;

        if (cmd.hasOption("hll")) {
            Files.list(Paths.get(profiler.directory))
                    .filter(f -> FilenameUtils.isExtension(f.toString(), "csv"))
                    .filter(Files::isRegularFile)
                    .forEach(f -> tablenameFilepathMap.put(
                            "\"" + FilenameUtils.getBaseName(f.toString()).trim().replaceAll("\"", "") + "\"",
                            f.toString()));

            HllProfiler hllProfiler = new HllProfiler();

            for (Map.Entry<String, String> entry : tablenameFilepathMap.entrySet()) {
                System.err.println(String.format("hll profiling %s", entry.getValue()));

                hllProfiler.createHllsFromCsv(entry.getValue(), profiler.keySize);

            }
            hllProfiler.findFieldIntersections();
            //make a second pass, and create hll's of potential foreign key permutations.
            hllProfiler.findForeignKeys();

        }

        else if (!cmd.hasOption("vo")) {

            Files.list(Paths.get(profiler.directory))
                    .filter(f -> FilenameUtils.isExtension(f.toString(), "csv"))
                    .filter(Files::isRegularFile)
                    .forEach(f -> tablenameFilepathMap.put(
                            "\"" + FilenameUtils.getBaseName(f.toString()).trim().replaceAll("\"", "") + "\"",
                            f.toString()));
            profiler.createInformationTables(connection);
            StopWatch stopWatch = StopWatch.createStarted();
            for (Map.Entry<String, String> entry : tablenameFilepathMap.entrySet()) {
                System.err.println(String.format("populating table from %s", entry.getValue()));
                columns = profiler.createTableFromCsv(entry.getKey(), entry.getValue(), connection);
                System.err.println(
                        String.format("determining field cardinalities for %s", entry.getKey()));
                profiler.determineCardinalities(entry.getKey(), columns, connection);
                System.err.println(
                        String.format("determining field data types for %s", entry.getKey()));
                profiler.determineFieldDatatypes(entry.getKey(), columns, connection);
                System.err.println(String.format("determining unique keys for %s", entry.getKey()));
                profiler.findUniqueKeys(entry.getKey(), columns, connection, profiler.keySize);
            }
            System.err.println("finding field intersections");
            profiler.findFieldIntersections(connection, profiler.inclusionCoefficient);
            System.err.println("finding foreign keys");
            for (Map.Entry<String, String> entry : tablenameFilepathMap.entrySet()) {
                profiler.findForeignKeys(entry.getKey(), connection, profiler.inclusionCoefficient);
            }
            stopWatch.stop();
            System.err.println("finished in " + stopWatch.getTime() / 1000 + " seconds");
        }

        if (cmd.hasOption("oj")) {
            System.out.println(profiler.getJsonInformationSchema(connection));
        }
        else if (cmd.hasOption("ox")) {
            System.out.println(profiler.getXmlInformationSchema(connection));
        }

        if (cmd.hasOption("og")) {
            profiler.drawGraph(connection);
        }

    }

    private static CommandLine processCliArgs(String[] args, CsvProfiler profiler)
            throws ParseException {

        cliOptions.addOption("d", true,
                "directory containing csv files, default: current directory");
        cliOptions.addOption("db", true,
                "sqlite database (.db), file default: <current path>/csvprofiler.db");
        cliOptions.addOption("k", true, "maximum unique key size (1 - 5), default: 2");
        cliOptions.addOption("kd", true, "minimum key density non-null count/ total count, 0.0 - 1.0, defalt 1.0");
        cliOptions.addOption("i", true,
                "foreign key inclusion coefficient (0.0 - 1.0), default: 0.80");
        cliOptions.addOption("s", true, "sample coefficient (0.10 - 1.0), default: 1.0");
        cliOptions.addOption("l", true, "limit number of lines to read and perform radom selection, default 100000");
        cliOptions.addOption("vo", false,
                "visualize foreign key graph only, from the specified database");
        cliOptions.addOption("help", false, "display this message");
        cliOptions.addOption("h", false, "display this message");
        cliOptions.addOption("oj", false, "output information schema as json");
        cliOptions.addOption("ox", false, "output information schema as xml");
        cliOptions.addOption("og", false, "draw foreign key dependency graph");
        cliOptions.addOption("hll", false, "hyperloglog key discovery");

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(cliOptions, args);

        if (cmd.hasOption("d")) {
            profiler.directory = cmd.getOptionValue("d");
        }
        if (cmd.hasOption("db")) {
            profiler.databaseFilePath = cmd.getOptionValue("db");
        }
        if (cmd.hasOption("k")) {
            profiler.keySize = Integer.parseInt(cmd.getOptionValue("k"));
        }
        if (cmd.hasOption("kd")) {
            profiler.minKeyDensity = Double.parseDouble(cmd.getOptionValue("kd"));
        }
        if (cmd.hasOption("i")) {
            profiler.inclusionCoefficient = Double.parseDouble(cmd.getOptionValue("i"));
        }
        if (cmd.hasOption("s")) {
            profiler.sampleCoefficient = Double.parseDouble(cmd.getOptionValue("s"));
            profiler.inclusionCoefficient *= Math.pow(profiler.sampleCoefficient,2);
        }
        if (cmd.hasOption("l")) {
            profiler.sampleLimit = Integer.parseInt(cmd.getOptionValue("l"));
        }
        if (cmd.hasOption("h") || cmd.hasOption("help")) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("java -jar <path-to-jar>", cliOptions);
            System.exit(0);
        }

        return cmd;

    }
}
