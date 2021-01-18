package com.mkf.profiler;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.math.BigInteger;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.graphstream.graph.Edge;
import org.graphstream.graph.Graph;
import org.graphstream.graph.implementations.MultiGraph;
import org.graphstream.ui.layout.HierarchicalLayout;
import org.graphstream.ui.layout.Layout;

import com.google.common.collect.Lists;
import com.google.common.math.BigIntegerMath;


class CsvProfiler {

    private final Map<String, Set<Set<String>>> uniqueKeyMap = new HashMap<>(); // tableName, Set of Set
    // of fields

    private final Map<String, Double> sampleReductionMap = new HashMap<>();
    int keySize = 2;
    int sampleLimit = -1;
    double inclusionCoefficient = 0.80;
    double sampleCoefficient = 1.0;
    String databaseFilePath = "csvprofiler.db";
    String directory = ".";
    double minKeyDensity = 1.0;


    public void findFieldIntersections(Connection connection, double inclusionCoefficient)
            throws SQLException {

        String cartesianQueryString = "select field1.csv_name csv_name1, field1.name name1, field1.not_null_count not_null_count1, field1.cardinality cardinality1, csv1.cardinality,\n"
                + "\tfield2.csv_name csv_name2, field2.name name2, field2.not_null_count not_null_count2,field2.cardinality cardinality2, csv2.cardinality\n"
                + "\tfrom field field1 join csv csv1 on (field1.csv_name = csv1.name) ,\n"
                + "\tfield field2  join csv csv2 on (field2.csv_name = csv2.name)\n"
                + "\twhere field1.data_type = field2.data_type\n" + "\tand csv_name1 != csv_name2\n"
                + "\tand field1.not_null_count >= " + inclusionCoefficient + "* csv1.cardinality * csv1.sample_reduction_coefficient * csv2.sample_reduction_coefficient\n"
                + "\tand field2.not_null_count >= " + inclusionCoefficient + "* csv2.cardinality * csv1.sample_reduction_coefficient * csv2.sample_reduction_coefficient";

        Statement cartesianQuery = connection.createStatement();
        Statement intersectionQuery = connection.createStatement();
        ResultSet crs = cartesianQuery.executeQuery(cartesianQueryString);

        Set<Set<String>> checkedPairs = new HashSet<>();

        while (crs.next()) {

            Set<String> pair = new HashSet<>();

            String csvName1 = crs.getString("csv_name1");
            String fieldName1 = crs.getString("name1");
            int cardinality1 = (int) (inclusionCoefficient * sampleReductionMap.get(csvName1) * crs.getInt("cardinality1"));
            String csvName2 = crs.getString("csv_name2");
            String fieldName2 = crs.getString("name2");
            int cardinality2 = (int) (inclusionCoefficient * sampleReductionMap.get(csvName2) * crs.getInt("cardinality2"));

            pair.add(csvName1 + "." + fieldName1);
            pair.add(csvName2 + "." + fieldName2);

            PreparedStatement insertIntersection = connection.prepareStatement(
                    "insert into intersection (referent_csv_name, referent_field_name, dependent_csv_name, dependent_field_name) values (?,?,?,?)");

            if (checkedPairs.add(pair)) {

                String intersectionQueryString = "with intersection as "
                        + "(select count (1) intersection_cardinality " + "from (select "
                        + fieldName1 + " from " + csvName1 + " intersect select " + fieldName2
                        + " from " + csvName2 + "))"
                        + "select csv_name, name, cardinality, intersection.intersection_cardinality from field join intersection on (field.cardinality >= "
                        + inclusionCoefficient * sampleReductionMap.get(csvName1) * sampleReductionMap.get(csvName2) + " * intersection.intersection_cardinality) "
                        + "where ((csv_name = '" + csvName1 + "' and name = '" + fieldName1
                        + "') or (csv_name = '" + csvName2 + "' and name = '" + fieldName2 + "'))";

                ResultSet irs = intersectionQuery.executeQuery(intersectionQueryString);
                while (irs.next()) {
                    insertIntersection.clearParameters();
                    int intersectionCardinality = irs.getInt("intersection_cardinality");
                    String csvName = irs.getString("csv_name");
                    String fieldName = irs.getString("name");
                    if (csvName1.equals(csvName) && fieldName1.equals(fieldName)
                            && intersectionCardinality >= cardinality1) {
                        insertIntersection.setString(1, csvName2);
                        insertIntersection.setString(2, fieldName2);
                        insertIntersection.setString(3, csvName1);
                        insertIntersection.setString(4, fieldName1);
                        insertIntersection.executeUpdate();
                    } else if (csvName2.equals(csvName) && fieldName2.equals(fieldName)
                            && intersectionCardinality >= cardinality2) {
                        insertIntersection.setString(1, csvName1);
                        insertIntersection.setString(2, fieldName1);
                        insertIntersection.setString(3, csvName2);
                        insertIntersection.setString(4, fieldName2);
                        insertIntersection.executeUpdate();
                    }
                }
            }
        }
        connection.commit();
        cartesianQuery.close();
        intersectionQuery.close();
    }

    public void determineFieldDatatypes(String tableName, ArrayList<String> columns,
                                        Connection connection) throws SQLException {

        for (String field : columns) {
            PreparedStatement updateRealDatatype = connection
                    .prepareStatement("update field set data_type = ? "
                            + "where csv_name = ? and name = ? and not_null_count != 0 and not_null_count = "
                            + "(select sum( printf('%G', " + field + ") == printf('%s', " + field
                            + "))" + " not_null_count from " + tableName + ")");

            PreparedStatement updateIntegerDatatype = connection
                    .prepareStatement("update field set data_type = ? "
                            + "where csv_name = ? and name = ? and not_null_count != 0 and not_null_count = "
                            + "(select sum( printf('%d', " + field + ") == printf('%s', " + field
                            + "))" + " not_null_count from " + tableName + ")");

            updateIntegerDatatype.setString(1, "integer");
            updateIntegerDatatype.setString(2, tableName);
            updateIntegerDatatype.setString(3, field);

            if (updateIntegerDatatype.executeUpdate() == 0) {
                updateRealDatatype.setString(1, "real");
                updateRealDatatype.setString(2, tableName);
                updateRealDatatype.setString(3, field);
                updateRealDatatype.executeUpdate();
            }

        }

        connection.commit();
    }

    public void determineCardinalities(String tableName, List<String> columns,
                                       Connection connection) throws SQLException {

        Statement statement = connection.createStatement();
        StringJoiner fieldCountQuery = new StringJoiner(",", "select ",
                " , count(1) _total from " + tableName);
        Map<String, Integer> uniqueCountMap = new HashMap<>();
        columns.forEach(k -> fieldCountQuery.add(
                "count(distinct " + k + ") " + k + ", " + " count(" + k + ") \"" + k.replaceAll("\"", "") + "_not_null\""));

        ResultSet rs = statement.executeQuery(fieldCountQuery.toString());

        final ResultSetMetaData meta = rs.getMetaData();
        final int columnCount = meta.getColumnCount();
        for (int i = 1; i <= columnCount; i++) {
            uniqueCountMap.put(meta.getColumnName(i), rs.getInt(i));
        }

        int totalCount = uniqueCountMap.get("_total");
        PreparedStatement tableCardinalityUpdate = connection
                .prepareStatement("update csv set cardinality = ? where name = ?");
        PreparedStatement fieldCardinalityUpdate = connection.prepareStatement(
                "update field set cardinality = ? where csv_name = ? and name = ?");
        PreparedStatement fieldCountUpdate = connection.prepareStatement(
                "update field set not_null_count = ? where csv_name = ? and name = ?");

        // put counts on table
        for (Map.Entry<String, Integer> entry : uniqueCountMap.entrySet()) {
            String fieldName = entry.getKey();
            int distinctCount = entry.getValue();

            if (fieldName.equals("_total")) {
                tableCardinalityUpdate.clearParameters();
                tableCardinalityUpdate.setInt(1, totalCount);
                tableCardinalityUpdate.setString(2, tableName);
                tableCardinalityUpdate.executeUpdate();
            }
            // update field/cardinality with the cardinality count.
            else if (!fieldName.contains("_not_null")) {
                fieldCardinalityUpdate.clearParameters();
                fieldCardinalityUpdate.setInt(1, distinctCount);
                fieldCardinalityUpdate.setString(2, tableName);
                fieldCardinalityUpdate.setString(3, "\"" + fieldName + "\"");
                fieldCardinalityUpdate.executeUpdate();
            } else {
                fieldCountUpdate.clearParameters();
                fieldCountUpdate.setInt(1, distinctCount);
                fieldCountUpdate.setString(2, tableName);
                fieldCountUpdate.setString(3, "\"" + fieldName.replace("_not_null", "") + "\"");
                fieldCountUpdate.executeUpdate();
            }

        }

        connection.commit();
        tableCardinalityUpdate.close();
        fieldCardinalityUpdate.close();
        fieldCountUpdate.close();
        statement.close();

    }

    public ArrayList<String> createTableFromCsv(String tableName, String filePath,
                                                Connection connection) throws IOException, SQLException {

        ArrayList<String> columnNameList = new ArrayList<>();
        Statement statement = connection.createStatement();
        PreparedStatement preparedStatement = null;
        Reader in = new FileReader(filePath);
        Iterable<CSVRecord> records = CSVFormat.RFC4180.parse(in);
        sampleReductionMap.put(tableName, 1.0);

        StringJoiner ddl = new StringJoiner(" text,", "create table " + tableName + " (", " )");
        StringJoiner ps = new StringJoiner(",", "insert into " + tableName + " (", ") values");
        StringJoiner psArgs = new StringJoiner(",", "(", ")");
        for (CSVRecord record : records) {

            if (record.getRecordNumber() == 1) {
                statement.executeUpdate(String.format("insert into csv (name, sample_reduction_coefficient) values ('%s', 1.0)", tableName));

                for (int i = 0; i < record.size(); i++) {
                    String columnName = "\"" + record.get(i).replaceAll("\"", "").trim() + "\"";
                    columnNameList.add(columnName);
                    ddl.add(columnName);
                    ps.add(columnName);
                    psArgs.add("nullif(?,'')");
                    statement.executeUpdate("insert into field (name, csv_name, ordinal ) values ('"
                            + columnName + "','" + tableName + "', " + (i + 1) + ")");

                }

                statement.execute("drop table if exists " + tableName);
                statement.execute(ddl.toString());

                try {
                    preparedStatement = connection.prepareStatement(ps + " " + psArgs);
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            } else {

                for (int i = 0; i < record.size(); i++) {
                    if (preparedStatement != null) {
                        preparedStatement.setString(i + 1, record.get(i));
                    } else {
                        System.err.println("skipped");
                    }
                }

                if (preparedStatement != null) {
                    preparedStatement.executeUpdate();
                } else {
                    System.err.println("skipped");
                }
            }
        }
        statement.close();
        connection.commit();

        if (sampleCoefficient < 1.0 || sampleLimit > 0) {
            createTableSample(tableName, connection);
        }

        return columnNameList;

    }

    private void createTableSample(String tableName, Connection connection) throws SQLException {
        double sampleReductionCoefficient = 1.0;
        int sampleRows = 0;
        int totalRows;
        Statement statement = connection.createStatement();
        ResultSet rs = statement
                .executeQuery(String.format("select count(1) rows from %s", tableName));
        while (rs.next()) {
            totalRows = rs.getInt("rows");
            if (sampleCoefficient < 1.0) {
                sampleRows = Math.max((int) (totalRows * sampleCoefficient), 10);
            } else {
                sampleRows = Math.min(totalRows, sampleLimit);
                sampleReductionCoefficient = sampleRows * 1.0 / totalRows;
                sampleReductionMap.put(tableName, sampleReductionCoefficient);
                System.err.println("Sample limit " + sampleLimit + " sample rows " + sampleRows + " total rows " + totalRows);
            }
            break;
        }

        PreparedStatement sampleReductionUpdate = connection
                .prepareStatement("update csv set sample_reduction_coefficient = ? where name = ?");
        sampleReductionUpdate.setString(2, tableName);
        sampleReductionUpdate.setDouble(1, sampleReductionCoefficient);
        sampleReductionUpdate.executeUpdate();
        String rawTableName = "\"" + "raw_" + tableName.replaceAll("\"", "") + "\"";
        statement.execute(String.format("drop table if exists %s", rawTableName));
        statement.execute(String.format("alter table %s rename to %s", tableName, rawTableName));
        statement.executeUpdate(
                String.format("create table %s as select * from %s order by random() limit %d",
                        tableName, rawTableName, sampleRows));
        statement.executeUpdate(String.format("drop table %s", rawTableName));
        statement.close();
        connection.commit();
    }

    public void createInformationTables(Connection connection) throws SQLException {

        Statement statement = connection.createStatement();

        final String csvDdl = "CREATE TABLE csv "
                + "( name TEXT, cardinality INTEGER, sample_reduction_coefficient REAL, PRIMARY KEY(name))";
        final String fieldDdl = "CREATE TABLE field "
                + "( name TEXT, csv_name TEXT, ordinal INTEGER, data_type TEXT DEFAULT 'text', cardinality INTEGER, not_null_count integer, PRIMARY KEY(name,csv_name), FOREIGN KEY(csv_name) REFERENCES csv(name) ON DELETE CASCADE)";
        final String uniqueKeyDdl = "CREATE TABLE unique_key "
                + "( name TEXT, csv_name TEXT, PRIMARY KEY(name), FOREIGN KEY(csv_name) REFERENCES csv(name) ON DELETE CASCADE)";
        final String uniqueKeyFieldDdl = "CREATE TABLE unique_key_field "
                + "( unique_key_name TEXT, field_name TEXT, PRIMARY KEY(unique_key_name, field_name), FOREIGN KEY(unique_key_name) REFERENCES unique_key(name) ON DELETE CASCADE)";
        final String foreignKeyDdl = "CREATE TABLE foreign_key "
                + "( name TEXT, unique_key_name TEXT, dependent_csv_name TEXT, cardinality TEXT, inclusion_coefficient REAL, PRIMARY KEY(name), FOREIGN KEY(unique_key_name) REFERENCES unique_key(name) ON DELETE CASCADE, FOREIGN KEY (dependent_csv_name) REFERENCES csv(name) ON DELETE CASCADE)";
        final String foreignKeyFieldDdl = "CREATE table foreign_key_field "
                + "( foreign_key_name TEXT, unique_key_field_name TEXT, foreign_key_field_name TEXT, PRIMARY KEY( foreign_key_name, unique_key_field_name, foreign_key_field_name), FOREIGN KEY (foreign_key_name) REFERENCES foreign_key(name) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED)";
        final String intersectionDdl = "CREATE TABLE intersection "
                + "( referent_csv_name TEXT, referent_field_name TEXT, dependent_csv_name TEXT, dependent_field_name ,"
                + "PRIMARY KEY(referent_csv_name,referent_field_name,dependent_csv_name,dependent_field_name), FOREIGN KEY (referent_csv_name, referent_field_name) REFERENCES field(csv_name, name) ON DELETE CASCADE, FOREIGN KEY (dependent_csv_name, dependent_field_name) REFERENCES field(csv_name, name) ON DELETE CASCADE)";
        final String pkViewDdl = "create view unique_key_fields  (unique_key_name, csv_name, field_name)\n"
                + "as select unique_key.name, unique_key.csv_name, group_concat(unique_key_field.field_name,',') field_name\n"
                + "\tfrom unique_key join unique_key_field on (unique_key.name = unique_key_field.unique_key_name)\n"
                + "\tgroup by unique_key.name, unique_key.csv_name";
        final String pkFkViewDddl = "create view unique_key_foreign_key  \n"
                + "(unique_key_name, foreign_key_name,  referent_csv_name,  referent_field_name,  dependent_csv_name, dependent_field_name, dependent_cardinality, inclusion_coefficient)\n"
                + "as select unique_key.name, foreign_key.name, unique_key.csv_name, group_concat(unique_key_field.field_name), foreign_key.dependent_csv_name,\n"
                + "\tgroup_concat(foreign_key_field.foreign_key_field_name), foreign_key.cardinality, foreign_key.inclusion_coefficient\n"
                + "\tfrom unique_key join unique_key_field on (unique_key.name = unique_key_field.unique_key_name) \n"
                + "\tjoin foreign_key on (unique_key.name = foreign_key.unique_key_name) \n"
                + "\tjoin foreign_key_field on (foreign_key.name = foreign_key_field.foreign_key_name\n"
                + "\t\tand unique_key_field.field_name = foreign_key_field.unique_key_field_name)\n"
                + "\t\tgroup by unique_key.name, foreign_key.name, unique_key.csv_name, foreign_key.dependent_csv_name, foreign_key.cardinality, foreign_key.inclusion_coefficient";

        // drop tables in reverse order of foreign key dependencies
        statement.execute("drop table if exists foreign_key_field");
        statement.execute(foreignKeyFieldDdl);
        statement.execute("drop table if exists foreign_key");
        statement.execute(foreignKeyDdl);
        statement.execute("drop table if exists unique_key_field");
        statement.execute(uniqueKeyFieldDdl);
        statement.execute("drop table if exists unique_key");
        statement.execute(uniqueKeyDdl);
        statement.execute("drop table if exists intersection");
        statement.execute(intersectionDdl);
        statement.execute("drop table if exists field");
        statement.execute(fieldDdl);
        statement.execute("drop table if exists csv");
        statement.execute(csvDdl);
        statement.execute("drop view if exists unique_key_foreign_key");
        statement.execute(pkFkViewDddl);
        statement.execute("drop view if exists unique_key_fields");
        statement.execute(pkViewDdl);

    }

    public void findUniqueKeys(String tableName, ArrayList<String> columns, Connection connection,
                               int maxColumns) throws SQLException {

        BigInteger safeCombinationSize = BigInteger.valueOf(10000);
        int numFields = columns.size();
        int safeNumComposites = Math.min(maxColumns, numFields);

        while (getKeyCombinationSize(numFields, safeNumComposites)
                .compareTo(safeCombinationSize) > 0) {
            safeNumComposites--;
        }

        final int safeCompositeSize = safeNumComposites;

        Statement keyIndexDdl = connection.createStatement();

        PreparedStatement notNullColumnQuery = connection.prepareStatement(
                "select field.name from field join csv on (field.csv_name = csv.name) where csv.name = ? and field.not_null_count >= ? * csv.cardinality");
        notNullColumnQuery.setString(1, tableName);
        notNullColumnQuery.setDouble(2, minKeyDensity);
        ResultSet notNullColumnsRs = notNullColumnQuery.executeQuery();
        Set<String> columnSet = new HashSet<>(columns);
        while (notNullColumnsRs.next()) {
            columnSet.add(notNullColumnsRs.getString("name"));
        }
        notNullColumnQuery.close();
        Set<Set<String>> uniqueKeys = new HashSet<>();
        Set<Set<String>> candidateKeys = new HashSet<>();
        final boolean psetSuccess = candidateKeys.addAll(powerSet(columnSet, safeCompositeSize));

        // remove empty set, any sets larger than the max size
        if (psetSuccess) {
            candidateKeys.removeIf(s -> s.size() > safeCompositeSize || s.size() == 0);
        }

        Statement compositeKeyQuery = connection.createStatement();
        PreparedStatement uniqueKeyInsert = connection
                .prepareStatement("insert into unique_key (name, csv_name) values (?, ?)");
        PreparedStatement uniqueKeyFieldInsert = connection.prepareStatement(
                "insert into unique_key_field (unique_key_name, field_name) values (?, ?)");
        for (int i = 1; i <= safeCompositeSize; i++) {

            uniqueKeys.forEach(uk -> candidateKeys.removeIf(ck -> ck.containsAll(uk)));
            final int j = i;
            Set<Set<String>> candidateKeysBySize = candidateKeys.parallelStream().filter(s -> s.size() == j)
                    .collect(Collectors.toSet());
            for (Set<String> candidateKey : candidateKeysBySize) {
                String fieldNames = candidateKey.parallelStream().collect(Collectors.joining(", "));
                String query = "select " + fieldNames + ", count(1) from " + tableName
                        + " group by " + fieldNames + " having count(1) > 1 limit 1";
                ResultSet qrs = compositeKeyQuery.executeQuery(query);
                if (!qrs.next()) {
                    uniqueKeys.add(candidateKey);
                    if (uniqueKeyMap.containsKey(tableName)) {
                        uniqueKeyMap.get(tableName).add(candidateKey);
                    } else {
                        uniqueKeyMap.put(tableName,
                                new HashSet<>(Collections.singletonList(candidateKey)));
                    }

                    String uniqueKeyName = tableName + "." + candidateKey.size() + "."
                            + candidateKey.parallelStream().collect(Collectors.joining("."));
                    uniqueKeyName = uniqueKeyName.replaceAll("[^A-Za-z0-9]", "_");
                    uniqueKeyInsert.clearParameters();
                    uniqueKeyInsert.setString(1, uniqueKeyName);
                    uniqueKeyInsert.setString(2, tableName);
                    uniqueKeyInsert.executeUpdate();

                    for (String fieldName : candidateKey) {
                        uniqueKeyFieldInsert.setString(1, uniqueKeyName);
                        uniqueKeyFieldInsert.setString(2, fieldName);
                        uniqueKeyFieldInsert.executeUpdate();
                    }
                    // create index on unique key
                    keyIndexDdl.execute("create index " + uniqueKeyName + " on "
                            + tableName + "(" + String.join(",", candidateKey) + ")");
                }
                qrs.close();
            }
        }
        connection.commit();
    }

    public void findForeignKeys(String referentTableName, Connection connection,
                                double inclusionCoefficient) throws SQLException {

        String foreignKeyInsertString = "insert into foreign_key (name, unique_key_name, dependent_csv_name, cardinality, inclusion_coefficient) values (?,?,?,?,?)";
        String foreignKeyFieldInsertString = "insert into foreign_key_field (foreign_key_name, unique_key_field_name, foreign_key_field_name) values (?,?,?)";
        PreparedStatement foreignKeyInsert = connection.prepareStatement(foreignKeyInsertString);
        PreparedStatement foreignKeyFieldInsert = connection
                .prepareStatement(foreignKeyFieldInsertString);

        String candidateFKQueryString = "with uk1 as "
                + " (select name, count(1) key_size from unique_key where csv_name = ? group by name)"
                + " select unique_key.name, unique_key.csv_name,  intersection.dependent_csv_name, uk1.key_size "
                + " from unique_key " + " join uk1 on (unique_key.name = uk1.name) "
                + " join intersection on (unique_key.csv_name = intersection.referent_csv_name) "
                + " group by 1,2,3,4 order by 1,2,3";

        PreparedStatement candidateFKQuery = connection.prepareStatement(candidateFKQueryString);
        candidateFKQuery.setString(1, referentTableName);
        ResultSet rs = candidateFKQuery.executeQuery();

        // For each PK/FK pair
        while (rs.next()) {

            String keyName = rs.getString("name");
            String dependentTable = rs.getString("dependent_csv_name");
            int keySize = rs.getInt("key_size");
            ArrayList<String> keyFields = new ArrayList<>(keySize);
            List<ArrayList<String>> foreignKeyFields = new ArrayList<>();

            // get PK fields
            String keyFieldsQueryString = "select field_name from unique_key_field join unique_key on "
                    + " (unique_key_field.unique_key_name = unique_key.name) join field on "
                    + " (unique_key.csv_name = field.csv_name and unique_key_field.field_name = field.name) "
                    + "where unique_key.name = ? order by ordinal asc";

            PreparedStatement keyFieldsQuery = connection.prepareStatement(keyFieldsQueryString);
            keyFieldsQuery.setString(1, keyName);
            ResultSet keyFieldRs = keyFieldsQuery.executeQuery();

            // for each PK field
            while (keyFieldRs.next()) {
                ArrayList<String> foreignKeyFieldList = new ArrayList<>();
                String referentFieldName = keyFieldRs.getString("field_name");
                keyFields.add(referentFieldName);
                //only pick potential FK fields that have no nulls
                String foreignKeyFieldQueryString = "select dependent_field_name from intersection join field on (intersection.dependent_csv_name = field.csv_name and intersection.dependent_field_name = field.name) join csv on (field.csv_name = csv.name) where field.not_null_count >= ? * csv.cardinality and referent_csv_name = ? and referent_field_name = ? and dependent_csv_name = ?";
                PreparedStatement foreignKeyFieldQuery = connection
                        .prepareStatement(foreignKeyFieldQueryString);
                foreignKeyFieldQuery.setDouble(1, minKeyDensity);
                foreignKeyFieldQuery.setString(2, referentTableName);
                foreignKeyFieldQuery.setString(3, referentFieldName);
                foreignKeyFieldQuery.setString(4, dependentTable);
                ResultSet foreignKeyFieldRs = foreignKeyFieldQuery.executeQuery();

                // find any fields in the dependent table which match the pk field
                while (foreignKeyFieldRs.next()) {
                    foreignKeyFieldList.add(foreignKeyFieldRs.getString("dependent_field_name"));

                }
                foreignKeyFields.add(foreignKeyFieldList);
            }

            // no list of potential fk fields can be empty
            if (foreignKeyFields.parallelStream().noneMatch(ArrayList::isEmpty)) {

                List<List<String>> foreignKeyCombinations = Lists
                        .cartesianProduct(foreignKeyFields);
                // only check fk combinations of unique keys.
                // eg [masterId, updateTimestamp] but not [masterId, masterId]
                for (List<String> foreignKey : foreignKeyCombinations) {
                    if (foreignKey.parallelStream().allMatch(new HashSet<>()::add)) {

                        StringJoiner sj = new StringJoiner(" and ", "(", ")");
                        for (int i = 0; i < keyFields.size(); i++) {
                            sj.add(referentTableName + "." + keyFields.get(i) + " = "
                                    + dependentTable + "." + foreignKey.get(i));
                        }

                        String foreignKeyIntersectionQueryString = String.format(
                                "with dependent as (select count(1) dependent_count from  (select 1 from %s  join  %s on %s)) "
                                        + "select *, dependent.dependent_count * 1.0 / csv.cardinality inclusion_coefficient from csv, dependent where name = ? and dependent.dependent_count >= (%f * csv.cardinality)",
                                dependentTable, referentTableName, sj.toString(),
                                inclusionCoefficient * sampleReductionMap.get(referentTableName));

                        PreparedStatement foreignKeyIntersectionQuery = connection
                                .prepareStatement(foreignKeyIntersectionQueryString);
                        foreignKeyIntersectionQuery.setString(1, dependentTable);
                        ResultSet pkfkRs = foreignKeyIntersectionQuery.executeQuery();

                        String foreignKeyName = keyName + "." + dependentTable + "." + foreignKey;

                        while (pkfkRs.next()) {

                            double pkFkInclusionCoefficient = pkfkRs
                                    .getDouble("inclusion_coefficient");
                            String fkCardinality;
                            Set<String> fkSet = new HashSet<>();
                            for (int i = 0; i < keyFields.size(); i++) {
                                foreignKeyFieldInsert.clearParameters();
                                foreignKeyFieldInsert.setString(1, foreignKeyName);
                                foreignKeyFieldInsert.setString(2, keyFields.get(i));
                                foreignKeyFieldInsert.setString(3, foreignKey.get(i));
                                foreignKeyFieldInsert.executeUpdate();

                                fkSet.add(foreignKey.get(i));
                            }

                            if (uniqueKeyMap.containsKey(dependentTable)
                                    && uniqueKeyMap.get(dependentTable).contains(fkSet)) {
                                fkCardinality = "1";
                            } else {
                                fkCardinality = "m";
                            }

                            foreignKeyInsert.clearParameters();
                            foreignKeyInsert.setString(1, foreignKeyName);
                            foreignKeyInsert.setString(2, keyName);
                            foreignKeyInsert.setString(3, dependentTable);
                            foreignKeyInsert.setString(4, fkCardinality);
                            foreignKeyInsert.setDouble(5, pkFkInclusionCoefficient);
                            foreignKeyInsert.executeUpdate();

                        }

                    }
                }
            }
        }

        connection.commit();
    }

    private ObjectNode getInformationSchemaAsJson(Connection connection) throws Exception {

        List<String> informationSchemaTables = new ArrayList<>(Arrays.asList("csv", "field", "foreign_key", "foreign_key_field", /*"intersection",*/ "unique_key", "unique_key_field", "unique_key_fields", "unique_key_foreign_key"));
        Statement informationTableQuery = connection.createStatement();
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode informationSchemaJson = mapper.createObjectNode();

        for (String informationSchemaTable : informationSchemaTables) {

            ArrayNode arrayNode = mapper.createArrayNode();
            ResultSet iqrs = informationTableQuery.executeQuery("select * from " + informationSchemaTable);
            ResultSetMetaData rsmd = iqrs.getMetaData();

            while (iqrs.next()) {

                int columnCount = rsmd.getColumnCount();
                ObjectNode objectNode = mapper.createObjectNode();
                for (int i = 1; i <= columnCount; i++) {
                    objectNode.put(rsmd.getColumnName(i), iqrs.getObject(i).toString().replaceAll("\"",""));
                }
                arrayNode.add(objectNode);
            }

            informationSchemaJson.set(informationSchemaTable,arrayNode);
        }

        return informationSchemaJson;
    }

    public String getJsonInformationSchema(Connection connection) throws Exception {

        ObjectMapper mapper = new ObjectMapper();
        return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(getInformationSchemaAsJson(connection));
    }

    public String getXmlInformationSchema(Connection connection) throws Exception {

        XmlMapper xmlMapper = new XmlMapper();
        return xmlMapper.writerWithDefaultPrettyPrinter().withRootName("information_schema").writeValueAsString(getInformationSchemaAsJson(connection));
    }


    private static <T> Set<Set<T>> powerSet(Set<T> originalSet, int maxColumns) {
        Set<Set<T>> sets = new HashSet<>();
        if (maxColumns == 1) {
            originalSet.forEach(s -> sets.add(new HashSet<>(Collections.singletonList(s))));
            return sets;
        }
        if (originalSet.isEmpty()) {
            sets.add(new HashSet<>());
            return sets;
        }
        List<T> list = new ArrayList<>(originalSet);
        T head = list.get(0);
        Set<T> rest = new HashSet<>(list.subList(1, list.size()));
        for (Set<T> set : powerSet(rest, maxColumns)) {
            Set<T> newSet = new HashSet<>();
            newSet.add(head);
            newSet.addAll(set);
            if (set.size() <= maxColumns) {
                sets.add(set);
            }
            if (newSet.size() <= maxColumns) {
                sets.add(newSet);
            }
        }
        return sets;
    }

    public void drawGraph(Connection connection) throws SQLException {

        Graph graph = new MultiGraph("ERD");
        // Graph m = new
        Statement query = connection.createStatement();
        ResultSet nodeRs = query.executeQuery("select name from csv");

        while (nodeRs.next()) {
            String csvName = nodeRs.getString("name");
            graph.addNode(csvName);
            graph.getNode(csvName).setAttribute("ui.label", csvName);
            graph.getNode(csvName).setAttribute("layout.weight", 4);
            graph.getNode(csvName).setAttribute("ui.style",
                    "text-background-mode:plain;shape:box;stroke-mode:plain;fill-mode:none;size-mode: fit; text-alignment: center;");
        }

        ResultSet edgeRs = query.executeQuery("select * from unique_key_foreign_key");

        while (edgeRs.next()) {
            String referent = edgeRs.getString("referent_csv_name");
            String dependent = edgeRs.getString("dependent_csv_name");
            String edgeName = referent + "->" + dependent;
            String reverseEdgeName = dependent + "->" + referent;
            if (graph.getEdge(edgeName) == null && graph.getEdge(reverseEdgeName) == null) {
                Edge edge = graph.addEdge(edgeName, referent, dependent, true);
                edge.setAttribute("layout.weight", 4);
                if (edgeRs.getString("dependent_cardinality").equals("m")) {
                    edge.setAttribute("ui.style", "arrow-shape:diamond;");
                }
            }
        }
        graph.addAttribute("ui.quality");
        graph.addAttribute("ui.antialias");

        Layout layout = new HierarchicalLayout();
        graph.addSink(layout);
        layout.addAttributeSink(graph);

        graph.display();
    }

    private static BigInteger getKeyCombinationSize(int numFields, int compositeKeySize) {

        BigInteger keyCombinations = BigInteger.valueOf(0);

        for (int r = compositeKeySize; r > 0; r--) {
            keyCombinations = keyCombinations
                    .add(BigIntegerMath.factorial(numFields).divide(BigIntegerMath.factorial(r)
                            .multiply(BigIntegerMath.factorial(Math.max(numFields - r, 1)))));
        }
        return keyCombinations;
    }
}
