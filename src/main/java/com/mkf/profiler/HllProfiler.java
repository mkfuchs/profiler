package com.mkf.profiler;

import com.google.common.collect.Collections2;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.google.common.math.BigIntegerMath;
import net.agkn.hll.HLL;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.math.BigInteger;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toMap;

public class HllProfiler {

    double minKeyDensity = 1.0;
    double inclusionCoefficient = 0.80;

    //get the power set, then permute each element of the powerset.
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

    private static BigInteger getKeyCombinationSize(int numFields, int compositeKeySize) {
        BigInteger keyCombinations = BigInteger.valueOf(0);

        for (int r = compositeKeySize; r > 0; r--) {
            keyCombinations = keyCombinations
                    .add(BigIntegerMath.factorial(numFields).divide(BigIntegerMath.factorial(r)
                            .multiply(BigIntegerMath.factorial(Math.max(numFields - r, 1)))));
        }
        return keyCombinations;
    }

    public ArrayList<String> discoverFieldProperties(Connection connection, String dataSourceLabel, String filePath, String dataSourceName, int keyArity) throws IOException, SQLException {

        HashFunction hasher = Hashing.murmur3_128();
        Random random = new Random();
        Statement statement = connection.createStatement();
        BigInteger safeCombinationSize = BigInteger.valueOf(10000);
        ArrayList<String> columnNameList = new ArrayList<>();
        Reader in = new FileReader(filePath);
        Iterable<CSVRecord> records = CSVFormat.RFC4180.parse(in);

        int cols = 0;
        int lineCount = 0;
        int reservoirSize = 492;
        Map<Integer, HLL> hllFields = new HashMap<>();
        Map<Integer, List<String>> reservoir = new HashMap<>();
        Map<Integer, Integer> valueCount = new HashMap<>();


        PreparedStatement updateFieldStatement = connection.prepareStatement("update field set hll = ?, not_null_count = ?, cardinality = ?, density = ?, uniqueness = ?, data_type = ? where data_source_name = ? and ordinal = ?");
        PreparedStatement insertUniqueKeyStatement = connection.prepareStatement("insert into unique_key select data_source_name || '-' || ordinal, data_source_name, hll from field where data_source_name = ? and density > 0.95 and uniqueness > 0.95 and data_type != 'float'");
        PreparedStatement insertUniqueKeyFieldStatement = connection.prepareStatement("insert into unique_key_field select data_source_name || '-' || ordinal, data_source_name, ordinal from field where data_source_name = ? and density > 0.95 and uniqueness > 0.95 and data_type != 'float'");


        statement.executeUpdate(String.format("insert into data_source (name, label, location, cardinality) values ('%s', '%s', '%s', %d)", dataSourceName, dataSourceLabel, filePath, 0));
        connection.commit();

        for (CSVRecord record : records) {

            if (record.getRecordNumber() == 1) {
                cols = record.size();
                int safeNumComposites = Math.min(5, cols);

                while (getKeyCombinationSize(cols, safeNumComposites)
                        .compareTo(safeCombinationSize) > 0) {
                    safeNumComposites--;
                }

                keyArity = Math.min(safeNumComposites, keyArity);

                for (int i = 0; i < cols; i++) {
                    reservoir.put(i, new ArrayList<>());
                    columnNameList.add(record.get(i));
                    valueCount.put(i, 0);
                    hllFields.put(i, new HLL(13,5));

                    statement.executeUpdate(String.format("insert into field (label, data_source_name, ordinal, cardinality, not_null_count, data_type) values ('%s', '%s', %d, %d, %d, '%s')", record.get(i), dataSourceName, i, 0, 0, "string"));
                    connection.commit();
                }
            } else {

                lineCount++;
                //if field has value increment fieldValueCount
                for (int i = 0; i < cols; i++) {
                    String fieldValue = record.get(i);
                    if (!StringUtils.isBlank(fieldValue)) {
                        valueCount.put(i, valueCount.get(i) + 1);
                        hllFields.get(i).addRaw(hasher.hashUnencodedChars(fieldValue).asLong());
                        if (reservoir.get(i).size() < reservoirSize) {
                            if (!reservoir.get(i).contains(fieldValue)) {
                                reservoir.get(i).add(fieldValue);
                            }
                        }
                        else {
                            int randomInt = random.nextInt(lineCount);
                            if (randomInt < reservoirSize) {
                                if (!reservoir.get(i).contains(fieldValue)) {
                                    reservoir.get(i).remove(randomInt);
                                    reservoir.get(i).add(randomInt, fieldValue);
                                }
                            }
                        }
                    }
                }
            }
        }
        // update data source with lineCount
        statement.executeUpdate(String.format("update data_source set cardinality = %d where name = '%s'", lineCount, dataSourceName));
        connection.commit();

        //update field hlls in db
        for (Map.Entry<Integer, HLL> entry : hllFields.entrySet()) {
            Integer i = entry.getKey();
            HLL hll = entry.getValue();

            updateFieldStatement.setBytes(1, hll.toBytes());
            updateFieldStatement.setInt(2, valueCount.get(i));
            updateFieldStatement.setLong(3, hll.cardinality());
            updateFieldStatement.setDouble(4, lineCount == 0 ? 0 : valueCount.get(i) * 1.0 / lineCount);
            updateFieldStatement.setDouble(5, valueCount.get(i) == 0? 0 : hll.cardinality() * 1.0 / valueCount.get(i));
            updateFieldStatement.setString(6, determineFieldDataType(reservoir.get(i)));
            updateFieldStatement.setString(7, dataSourceName);
            updateFieldStatement.setInt(8, i);
            updateFieldStatement.executeUpdate();
            connection.commit();
        }

        // unary unique keys
        insertUniqueKeyStatement.setString(1,dataSourceName);
        insertUniqueKeyFieldStatement.setString(1,dataSourceName);
        insertUniqueKeyStatement.executeUpdate();
        insertUniqueKeyFieldStatement.executeUpdate();
        connection.commit();

        return columnNameList;
    }

    private String determineFieldDataType(List<String> sampleList) {
        int doubleCount = 0;
        int longCount = 0;
        String dataType = "string";

        if (sampleList.size() == 0) {
            return (dataType);
        }

        for (String s: sampleList) {
            if (s.contains(".")){
                try {
                    Double.parseDouble(s);
                    doubleCount++;
                }
                catch (Exception ignored) {}
            }
            try {
                Long.parseLong(s);
                longCount++;
            }
            catch (Exception ignored) {}
        }
        if (doubleCount * 1.0 / sampleList.size() > 0.8) {
            dataType = "float";
        }
        else if (longCount * 1.0 / sampleList.size() > 0.8) {
            dataType = "integer";
        }
        else {
            dataType = "string";
        }

        return(dataType);
    }

    public void discoverFieldIntersections(Connection connection) throws  SQLException{

        Statement statement = connection.createStatement();
        PreparedStatement intersectionQuery = connection.prepareStatement(
                "select referent.data_source_name referent_data_source_name, referent.ordinal referent_ordinal, referent.hll referent_hll_blob, dependent.data_source_name dependent_data_source_name, dependent.ordinal dependent_ordinal, dependent.cardinality dependent_cardinality, dependent.hll dependent_hll_blob " +
                "from field referent join field dependent on (referent.data_source_name != dependent.data_source_name and referent.data_type != 'float' and dependent.data_type = referent.data_type and referent.density > 0.95 and dependent.density > 0.95)");

        String intersectionStatement = "insert into intersection " +
                        "(referent_data_source_name, referent_field_ordinal, " +
                        "dependent_data_source_name, dependent_field_ordinal, " +
                        "intersection_cardinality, inclusion_coefficient) values ('%s', %d, '%s', %d, %d, %f)";


        ResultSet intersectionRs = intersectionQuery.executeQuery();
        while (intersectionRs.next()) {

            String referentDataSourceName = intersectionRs.getString("referent_data_source_name");
            int referentOrdinal =intersectionRs.getInt("referent_ordinal");
            byte[] referentHllBlob = intersectionRs.getBytes("referent_hll_blob");
            HLL referentHll = HLL.fromBytes(referentHllBlob);

            String dependentDataSourceName = intersectionRs.getString("dependent_data_source_name");
            int dependentOrdinal =intersectionRs.getInt("dependent_ordinal");
            int dependentCardinality = intersectionRs.getInt("dependent_cardinality");
            byte[] dependentHllBlob = intersectionRs.getBytes("dependent_hll_blob");
            HLL hll2 = HLL.fromBytes(dependentHllBlob);

            long bothCardinality = referentHll.cardinality() + hll2.cardinality();
            referentHll.union(hll2);
            long intersectionCardinality = bothCardinality - referentHll.cardinality();
            double inclusionCoefficient = intersectionCardinality * 1.0 / dependentCardinality;

            if (intersectionCardinality > 10 && inclusionCoefficient > 0.95) { //added the interstion cardinality
                statement.executeUpdate(String.format(intersectionStatement,
                        referentDataSourceName, referentOrdinal, dependentDataSourceName, dependentOrdinal, intersectionCardinality, inclusionCoefficient));
            }
        }
        connection.commit();
    }

    public void discoverUnaryForeignKeys(Connection connection) throws SQLException {

        Statement statement = connection.createStatement();
        statement.executeUpdate(
                "insert into foreign_key (name, unique_key_name, data_source_name, inclusion_coefficient) " +
                        "select ukf.unique_key_name || ':' || ixn.dependent_data_source_name || '-' || ixn.dependent_field_ordinal, \n" +
                        "\tukf.unique_key_name, ixn.dependent_data_source_name, ixn.inclusion_coefficient\n" +
                        "\t from unique_key_field ukf join intersection ixn \n" +
                        "\t on (ukf.data_source_name = ixn.referent_data_source_name and ukf.field_ordinal = ixn.referent_field_ordinal) \n"
                        //"\t where ixn.intersection_cardinality > 10"//mike testing
        );
        statement.executeUpdate(
                "insert into foreign_key_field " +
                        "(foreign_key_name, referent_data_source_name, referent_field_ordinal, " +
                        "dependent_data_source_name, dependent_field_ordinal, unique_key_name)" +
                        "select ukf.unique_key_name || ':' || ixn.dependent_data_source_name || '-' || ixn.dependent_field_ordinal foreign_key_name, \n" +
                        "\t ixn.referent_data_source_name, ixn.referent_field_ordinal, ixn.dependent_data_source_name, ixn.dependent_field_ordinal,\n" +
                        "\t ukf.unique_key_name\n" +
                        "\t from unique_key_field ukf join intersection ixn \n" +
                        "\t on (ukf.data_source_name = ixn.referent_data_source_name and ukf.field_ordinal = ixn.referent_field_ordinal)"
        );
        statement.executeUpdate(
                "update foreign_key set cardinality = '1:m' where name in\t \n" +
                        "(select foreign_key_name from foreign_key_field \n" +
                        "where (dependent_data_source_name, dependent_field_ordinal) not in \n" +
                        "(select data_source_name, field_ordinal from unique_key_field))"
        );
        connection.commit();
    }

    public void discoverCompositeKeys(Connection connection, String filePath, String dataSourceName,
                                      int dataSourceCardinality, int keyArity) throws IOException, SQLException {

        HashFunction hasher = Hashing.murmur3_128();
        Statement statement = connection.createStatement();
        BigInteger safeCombinationSize = BigInteger.valueOf(10000);
        Map<List<Integer>, HLL> hllCombinations = new HashMap<>();
        Set<Set<Integer>> powerSet;
        StringBuilder combinationSb = new StringBuilder();
        Reader in = new FileReader(filePath);
        Iterable<CSVRecord> records = CSVFormat.RFC4180.parse(in);

        //query db to get lines and columns to use. filter out floats, density <= 0.95
        List<Integer> fieldList= new ArrayList<>();
        ResultSet fieldRs = statement.executeQuery(
                String.format("select ordinal from field where data_source_name = '%s' and density > 0.95 and uniqueness <= 0.95" +
                        " and data_type != 'float' and ordinal not in " +
                        "(select distinct field_ordinal from unique_key_field where data_source_name = '%s')",
                        dataSourceName, dataSourceName));
        while (fieldRs.next()) {
            fieldList.add(fieldRs.getInt(1));
        }

        if (fieldList.size() <= 1) {
            //we have already identified single field unique keys
            return;
        }

        int cols = fieldList.size();
        int safeNumComposites = Math.min(5, cols);

        while (getKeyCombinationSize(cols, safeNumComposites)
                .compareTo(safeCombinationSize) > 0) {
            safeNumComposites--;
        }

        keyArity = Math.min(safeNumComposites, keyArity);
        powerSet = powerSet(new HashSet<>(fieldList), keyArity);
        powerSet.removeIf(s -> s.isEmpty() || s.size() == 1);

        for (Set<Integer> keyCombination : powerSet) {
            List<Integer> fieldCombination = new ArrayList<>(new TreeSet<>(keyCombination));
            hllCombinations.put(fieldCombination, new HLL(13, 5));
            //System.out.println(keyCombination.size());
        }

        //System.out.printf("%d composite keys can be made of these fields %s\n", powerSet.size(), fieldList);
        // read csv (skip first line)
        //prune combinations periodically
        int recordsSincePruning = 0;
        for (CSVRecord record : records) {
            if (record.getRecordNumber() == 1) {
            }
            // concatenate field values for every candidate key value and update its respective hll
            for (Set<Integer> keyCombination : powerSet) {
                List<Integer> fieldCombination = new ArrayList<>(new TreeSet<>(keyCombination));
                combinationSb.setLength(0);
                List<String> columnStings = new ArrayList<>();
                fieldCombination.forEach(col -> columnStings.add(record.get(col)));
                hllCombinations.get(fieldCombination).addRaw(hasher.hashUnencodedChars(String.join("-", columnStings)).asLong());
            }

            recordsSincePruning ++;

            if (record.getRecordNumber() == dataSourceCardinality || recordsSincePruning * 1.0 / dataSourceCardinality >= 0.15) {
                removeNonUniqueCombinations(hllCombinations, powerSet, record.getRecordNumber());
                recordsSincePruning = 0;
            }
        }
        //remove redundant keys (supersets of smaller keys)
        List<List<Integer>> candidateKeys = new ArrayList<>(hllCombinations.keySet());
        for (List<Integer> candidateKey : candidateKeys) {
            for (List<Integer> hllKey : candidateKeys) {
                if (hllKey.containsAll(candidateKey) && hllKey.size() > candidateKey.size()) {
                    hllCombinations.remove(hllKey);
                }
            }
        }

        //put them in the database
        PreparedStatement insertUniqueKeyStatement =
                connection.prepareStatement("insert into unique_key " +
                        "(name, data_source_name, hll) values (?, ?, ?)");
        PreparedStatement insertUniqueKeyFieldStatement =
                connection.prepareStatement("insert into unique_key_field " +
                        "(unique_key_name, data_source_name, field_ordinal) values (?, ?, ?)");

        for (Map.Entry<List<Integer>, HLL> entry : hllCombinations.entrySet()) {
            List<Integer> uniqueKey = entry.getKey();
            HLL hll = entry.getValue();
            String uniqueKeyName = dataSourceName + "-" +
                    String.join("-",uniqueKey.stream().map(i -> String.valueOf(i)).collect(Collectors.toList()));
            insertUniqueKeyStatement.setString(1, uniqueKeyName);
            insertUniqueKeyStatement.setString(2,dataSourceName);
            insertUniqueKeyStatement.setBytes(3, hll.toBytes());
            try {
                insertUniqueKeyStatement.executeUpdate();

                for (int fieldOrdinal : uniqueKey) {
                    insertUniqueKeyFieldStatement.setString(1,uniqueKeyName);
                    insertUniqueKeyFieldStatement.setString(2,dataSourceName);
                    insertUniqueKeyFieldStatement.setInt(3,fieldOrdinal);
                    insertUniqueKeyFieldStatement.executeUpdate();
                }
            }
            catch (Exception ignored) {
                System.out.printf("***uk insert failed for %s %s %s\n", dataSourceName, uniqueKey, uniqueKeyName );
            }
        }

        connection.commit();
    }

    private void removeNonUniqueCombinations(Map<List<Integer>, HLL> hllCombinations, Set<Set<Integer>> powerSet, long recordNumber) {
        List<Set<Integer>> powerSetRemoval = new ArrayList<>();
        for (Set<Integer> keyCombination : powerSet) {
            List<Integer> fieldCombination = new ArrayList<>(new TreeSet<>(keyCombination));
            if (hllCombinations.get(fieldCombination).cardinality() * 1.0 / recordNumber < 0.95) {
                hllCombinations.remove(fieldCombination);
                powerSetRemoval.add(keyCombination);
            }
        }
        powerSet.removeAll(powerSetRemoval);
    }

    public void discoverForeignKeys(Connection connection, String filePath, String dataSourceName,
                                    int dataSourceCardinality, int keyArity) throws IOException, SQLException {

        //get a list of n-ary unique keys to check against.

        //for each n-ary unqiue key field, get the list of field intersections, and build
        //a set of permutations (fk-candidates)
        //for each line of the file, update each permutation's hll

    }



    public static void main(String[] args) {

        int cols = 100;
        int keyArity = 3;

        List<Integer> colList = IntStream.rangeClosed(1, cols)
                .boxed().collect(Collectors.toList());


        Set<Set<Integer>> powerSet = powerSet(new HashSet<>(colList), keyArity);
        powerSet.removeIf(Set::isEmpty);

        List<List<Integer>> permutations = new ArrayList<>();
        //for each element, generate permutations
        powerSet.forEach(combination -> permutations.addAll(Collections2.permutations(combination)));
        Map<List<Integer>, HLL> hllPermutations = new HashMap<>();
        for (List<Integer> permutation : permutations) {
            hllPermutations.put(permutation, new HLL(13, 5));
        }
        System.out.println(powerSet.size() + " " + permutations.size());
        System.out.println(getKeyCombinationSize(cols, keyArity));
    }

    public void createInformationTables(Connection connection) throws SQLException {

        Statement statement = connection.createStatement();

        final String dataSourceDdl = "CREATE TABLE data_source\n" +
                "(\n" +
                "\tname text\n" +
                "\t\tconstraint data_source_pk\n" +
                "\t\t\tprimary key,\n" +
                "\tlabel text,\n" +
                "\tlocation text not null\n" +
                ", cardinality int default 0 not null)";
        final String fieldDdl = "CREATE TABLE field\n" +
                "(\n" +
                "\tlabel text,\n" +
                "\tdata_source_name text not null\n" +
                "\t\tconstraint field_fk_1a\n" +
                "\t\t\treferences data_source (name) \n" +
                "\t\t\t\ton update cascade on delete cascade,\n" +
                "\tordinal int not null,\n" +
                "\tcardinality int default 0 not null,\n" +
                "\tdensity real default 0 not null,\n" +
                "\tuniqueness real default 0 not null,\n" +
                "\tnot_null_count int default 0 not null,\n" +
                "\tdata_type text default 'TEXT' not null,\n" +
                "\thll blob,\n" +
                "\tconstraint field_pk\n" +
                "\t\tprimary key (data_source_name, ordinal)\n" +
                ")";
        final String uniqueKeyDdl = "CREATE TABLE unique_key\n" +
                "(\n" +
                "\tname text not null\n" +
                "\t\tconstraint unique_key_pk\n" +
                "\t\t\tprimary key,\n" +
                "\tdata_source_name text not null\n" +
                "\t\tconstraint uk_fk_1\n" +
                "\t\t\treferences data_source (name)\n" +
                "\t\t\t\ton delete cascade,\n" +
                "\thll blob\n" +
                ")";
        final String uniqueKeyFieldDdl = "CREATE TABLE unique_key_field\n" +
                "(\n" +
                "\tunique_key_name text\n" +
                "\t\tconstraint ukf_fk_1\n" +
                "\t\t\treferences unique_key (name)\n" +
                "\t\t\t\ton delete cascade,\n" +
                "\tdata_source_name text,\n" +
                "\tfield_ordinal int,\n" +
                "\tconstraint unique_key_field_pk\n" +
                "\t\tprimary key (unique_key_name, data_source_name, field_ordinal),\n" +
                "\tconstraint ukf_fk_2\n" +
                "\t\tforeign key (field_ordinal, data_source_name) references field (ordinal, data_source_name)\n" +
                "\t\t\ton delete cascade\n" +
                ")";
        final String foreignKeyDdl = "CREATE TABLE foreign_key\n" +
                "(\n" +
                "\tname text\n" +
                "\t\tconstraint foreign_key_pk\n" +
                "\t\t\tprimary key,\n" +
                "\tunique_key_name text\n" +
                "\t\tconstraint fk_fk_1\n" +
                "\t\t\treferences unique_key (name)\n" +
                "\t\t\t\ton delete cascade,\n" +
                "\tdata_source_name text\n" +
                "\t\tconstraint fk_fk_2\n" +
                "\t\t\treferences data_source (name)\n" +
                "\t\t\t\ton delete cascade,\n" +
                "\tcardinality text default '1:1' not null,\n" +
                "\tinclusion_coefficient real default 0 not null\n" +
                ")";
        final String foreignKeyFieldDdl = "CREATE TABLE foreign_key_field\n" +
                "(\n" +
                "\tforeign_key_name text\n" +
                "\t\tconstraint fkf_fk_1\n" +
                "\t\t\treferences foreign_key (name)\n" +
                "\t\t\t\ton delete cascade,\n" +
                "\treferent_data_source_name text,\n" +
                "\treferent_field_ordinal int,\n" +
                "\tdependent_data_source_name text,\n" +
                "\tdependent_field_ordinal int,\n" +
                "\tunique_key_name text,\n" +
                "\tconstraint foreign_key_field_pk\n" +
                "\t\tunique (foreign_key_name, unique_key_name, referent_data_source_name, referent_field_ordinal, dependent_data_source_name, dependent_field_ordinal),\n" +
                "\tconstraint fkf_fk_2\n" +
                "\t\tforeign key (dependent_data_source_name, dependent_field_ordinal) references field (data_source_name, ordinal)\n" +
                "\t\t\ton delete cascade,\n" +
                "\tconstraint fkf_fk_3\n" +
                "\t\tforeign key (unique_key_name, referent_data_source_name, referent_field_ordinal) references unique_key_field (unique_key_name, data_source_name, field_ordinal) \n" +
                "\t\t\ton delete cascade\n" +
                ")";
        final String intersectionDdl = "CREATE TABLE intersection\n" +
                "(\n" +
                "\treferent_data_source_name text,\n" +
                "\treferent_field_ordinal int,\n" +
                "\tdependent_data_source_name text,\n" +
                "\tdependent_field_ordinal int,\n" +
                "\tintersection_cardinality int default 0 not null,\n" +
                "\tinclusion_coefficient real default 0 not null,\n" +
                "\tconstraint intersection_pk\n" +
                "\t\tunique (referent_data_source_name, referent_field_ordinal, dependent_data_source_name, dependent_field_ordinal),\n" +
                "\tconstraint inter_fk_1\n" +
                "\t\tforeign key (referent_data_source_name, referent_field_ordinal) references field (data_source_name, ordinal) \n" +
                "\t\t\ton delete cascade,\n" +
                "\tconstraint inter_fk_2\n" +
                "\t\tforeign key (dependent_data_source_name, dependent_field_ordinal) references field (data_source_name, ordinal)\n" +
                "\t\t\ton delete cascade\n" +
                ")";

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
        statement.execute("drop table if exists data_source");
        statement.execute(dataSourceDdl);
        statement.execute("drop view if exists unique_key_foreign_key");
        statement.execute(pkFkViewDddl);
        statement.execute("drop view if exists unique_key_fields");
        statement.execute(pkViewDdl);

        connection.commit();

    }

}
