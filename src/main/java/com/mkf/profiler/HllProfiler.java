package com.mkf.profiler;

import com.google.common.collect.Collections2;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.google.common.math.BigIntegerMath;
import it.unimi.dsi.fastutil.Hash;
import net.agkn.hll.HLL;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringUtils;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.math.BigInteger;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toMap;

public class HllProfiler {

    int arity = 1;
    int columns = 1;

    int keySize = 2;
    double minKeyDensity = 1.0;
    double inclusionCoefficient= 0.80;

    List<String> columnNames = new ArrayList<>();
    Map<List<Integer>, HLL> permutationMap = new HashMap<>();

    Map<Integer,String> csv = new HashMap<>();
    Map<Integer,Integer> csvLineCount = new HashMap<>();

    Map<Integer, Map<Integer,String>> field = new HashMap<>();
    Map<Integer, Map<Integer, Integer>> fieldValueCount = new HashMap<>();
    Map<Integer, Map<Integer,HLL>> fieldHll = new HashMap<>();
    Map<Integer, Map<List<Integer>, HLL>> csvUniqueKey = new HashMap<>();
    Map<Integer, Map<Integer, Map<Integer, List<Integer>>>> intersectionField = new HashMap<>();

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

    public ArrayList<String> createHllsFromCsv(String filePath, int keyArity) throws IOException {

        HashFunction hasher = Hashing.murmur3_128();
        BigInteger safeCombinationSize = BigInteger.valueOf(10000);
        ArrayList<String> columnNameList = new ArrayList<>();
        Reader in = new FileReader(filePath);
        Iterable<CSVRecord> records = CSVFormat.RFC4180.parse(in);

        int cols = 0;
        int lineCount=0;
        Set<Set<Integer>> powerSet = new HashSet<>();
        List<List<Integer>> permutations = new ArrayList<>();
        Map<List<Integer>, HLL> hllPermutations = new HashMap<>();
        Map<List<Integer>, HLL> hllCombinations= new HashMap<>();
        StringBuilder permutationSb = new StringBuilder();
        StringBuilder combinationSb = new StringBuilder();

        csv.put(csv.size(),filePath);
        int csvIndex = csv.size() - 1;
        Map<Integer,Integer> valueCount = new HashMap<>();




        for (CSVRecord record : records) {

            if (record.getRecordNumber() == 1) {



                cols = record.size();

                int safeNumComposites = Math.min(5, cols);

                while (getKeyCombinationSize(cols, safeNumComposites)
                        .compareTo(safeCombinationSize) > 0) {
                    safeNumComposites--;
                }

                keyArity = Math.min(safeNumComposites, keyArity);

                List<Integer> colList = IntStream.rangeClosed(0, cols -1)
                        .boxed().collect(Collectors.toList());

                Map<Integer, String> columnNameMap = new HashMap<>();
                for (int i = 0; i < cols; i++) {
                    columnNameList.add(record.get(i));
                    columnNameMap.put(i, record.get(i));
                    valueCount.put(i,0);

                }

                field.put(csvIndex,columnNameMap);

                //set up the Map to hold the HLLs for all column permutations
                powerSet = powerSet(new HashSet<Integer>(colList), keyArity);
                powerSet.removeIf(s -> s.isEmpty());
                powerSet.forEach(combination -> permutations.addAll(Collections2.permutations(combination)));
                hllPermutations = new HashMap<>();
                for (List<Integer> permutation : permutations) {
                    hllPermutations.put(permutation, new HLL(13,5));
                }

                //set up map for hll column combinations

                for (Set<Integer> keyCombination : powerSet) {
                    List<Integer> fieldCombination = new ArrayList<>(new TreeSet<>(keyCombination));
                    hllCombinations.put(fieldCombination, new HLL(13,5));
                }



                //add other Lists for column stats - not null count

            } else {

                lineCount ++;

                //if field has value increment fieldValueCount
                for (int i = 0; i < cols; i++) {
                    if (!StringUtils.isBlank(record.get(i))) {
                        valueCount.put(i, valueCount.get(i) + 1);
                    }
                }


                /*for (List<Integer> permutation : hllPermutations.keySet()) {
                    permutationSb.setLength(0);
                    permutation.forEach(col -> permutationSb.append(record.get(col - 1)));
                    hllPermutations.get(permutation).addRaw(hasher.hashUnencodedChars(permutationSb.toString()).asLong());


                }*/

                for (Set<Integer> keyCombination : powerSet) {
                    List<Integer> fieldCombination = new ArrayList<>(new TreeSet<>(keyCombination));
                    combinationSb.setLength(0);
                    fieldCombination.forEach(col -> combinationSb.append(record.get(col)));
                    hllCombinations.get(fieldCombination).addRaw(hasher.hashUnencodedChars(combinationSb.toString()).asLong());
                }
           }
        }

        final int minKeyCount = (int) (minKeyDensity * 0.95 * lineCount);
        fieldValueCount.put(csvIndex, valueCount);
        //csvLineCount.add(csvIndex, lineCount);
        csvLineCount.put(csvIndex,lineCount);


        Map<Integer, HLL> hlls = new HashMap<>();

        //hllCombinations.entrySet().stream().filter(e -> e.getKey().size() == 1 && e.getValue().cardinality() >= minKeyCount).map(e -> hlls.set(e.getKey().get(0), e.getValue()));
        hllCombinations.entrySet().stream().filter(e -> e.getKey().size() == 1 ).forEach(e -> hlls.put(e.getKey().get(0),e.getValue()));
        //get rid of fields that aren;t dense enough (to many blank/null values)
        hlls.entrySet().removeIf(e -> fieldValueCount.get(csvIndex).get(e.getKey()) < minKeyDensity * 0.95);
        fieldHll.put(csvIndex, hlls);

        List<List<Integer>> candidateKeys = hllCombinations.entrySet().stream().filter(e -> e.getValue().cardinality() >= minKeyCount).map(Map.Entry::getKey).collect(Collectors.toCollection(ArrayList::new));
        hllCombinations.keySet().removeIf(k -> !candidateKeys.contains(k));
        Set<List<Integer>> hllKeys = hllCombinations.keySet();
        for (List<Integer> candidateKey : candidateKeys) {
            for (List<Integer> hllKey : candidateKeys) {
                if (hllKey.containsAll(candidateKey) && hllKey.size() > candidateKey.size()) {
                    hllCombinations.remove(hllKey);
                }
            }
        }

        csvUniqueKey.put(csvIndex,hllCombinations);

        hllCombinations.keySet().forEach(k -> System.out.println(csv.get(csvIndex) + " " + k));
        return columnNameList;

    }

    public void findFieldIntersections() {

        //initialize intersection Map.

        for (int dependentCsv : csv.keySet()) {
            Map<Integer, Map<Integer, List<Integer>>> dependentCsvMap = new HashMap<>();
            intersectionField.put(dependentCsv, dependentCsvMap);
            for (int referentCsv : csv.keySet()) {
                if (dependentCsv != referentCsv) {
                    Map<Integer, List<Integer>>  referentCsvMap = new HashMap<>();
                    intersectionField.get(dependentCsv).put(referentCsv,referentCsvMap);
                    for (int dependentField : field.get(dependentCsv).keySet()) {
                        List<Integer> referentFields = new ArrayList<>();
                        intersectionField.get(dependentCsv).get(referentCsv).put(dependentField,referentFields);

                    }
                }
            }

        }


        for (Integer referentCsv : fieldHll.keySet()) {
           for (Integer referentField : fieldHll.get(referentCsv).keySet()) {
                HLL referentFieldHll = fieldHll.get(referentCsv).get(referentField);

                for (Integer dependentCsv : fieldHll.keySet()) {

                    if (referentCsv != dependentCsv) {
                        for (Integer dependentField : fieldHll.get(dependentCsv).keySet()) {
                            HLL dependentFieldHll = fieldHll.get(dependentCsv).get(dependentField);
                            int inclusionLimit = (int) (dependentFieldHll.cardinality() * inclusionCoefficient * 0.95);
                            HLL unionHll = HLL.fromBytes(referentFieldHll.toBytes());//referentFieldHll.clone();
                            unionHll.union(dependentFieldHll);
                            //System.out.println((double)((referentFieldHll.cardinality() + dependentFieldHll.cardinality() - unionHll.cardinality()) / dependentFieldHll.cardinality()) );
                            //System.out.println("---" + unionHll.cardinality() + ":" + dependentFieldHll.cardinality() +":" + referentFieldHll.cardinality() +"--" +csv.get(referentCsv)+ "." + field.get(referentCsv).get(referentField) + "=" + referentFieldHll.cardinality() + "," + csv.get(dependentCsv)+ "." + field.get(dependentCsv).get(dependentField) + dependentFieldHll.cardinality() + " " + (referentFieldHll.cardinality() + dependentFieldHll.cardinality() - unionHll.cardinality()));
                            if (dependentFieldHll.cardinality() > 10 && referentFieldHll.cardinality() + dependentFieldHll.cardinality() - unionHll.cardinality() >= inclusionLimit) {
                                System.out.println("***" + csv.get(referentCsv)+ "." + field.get(referentCsv).get(referentField) + "=" + csv.get(dependentCsv)+ "." + field.get(dependentCsv).get(dependentField));
                                //System.out.println(dependentCsv + "." + dependentField + " " + referentCsv + "." + referentField);
                                intersectionField.get(dependentCsv).get(referentCsv).get(dependentField).add(referentField);

                            }

                        }
                    }

                }
            }
        }
    }

    public void findForeignKeys() {



    }


    public static void main(String[] args) {

        int cols = 100;
        int keyArity = 3;

        List<Integer> colList = IntStream.rangeClosed(1, cols)
                .boxed().collect(Collectors.toList());


        Set<Set<Integer>> powerSet = powerSet(new HashSet<Integer>(colList), keyArity);
        powerSet.removeIf(s -> s.isEmpty());

        List<List<Integer>> permutations = new ArrayList<>();
        //for each element, generate permutations
        powerSet.forEach(combination -> permutations.addAll(Collections2.permutations(combination)));
        Map<List<Integer>, HLL> hllPermutations = new HashMap<>();
        for (List<Integer> permutation : permutations) {
            hllPermutations.put(permutation, new HLL(13,5));
        }
        System.out.println(powerSet.size() + " " + permutations.size() );
        System.out.println(getKeyCombinationSize(cols,keyArity));
    }



}
