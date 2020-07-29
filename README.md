# CSV Profiler

This java utility profiles *.csv files in a specified directory. It determines data types (integer, real or text), and finds unique and foreign keys.

The results are stored in a sqlite db file, which can be viewed with the sqlite executable, or db browser (recommended) http://sqlitebrowser.org/
The following tables and views contain the profiling results: csv, field, unique_key, unique_key_field, foreign_key, foreign_key_field, intersection,
unique_key_foreign_key_field, unique_key_fields

The CSV profiler is a gradle project and is built by executing "gradle clean jar" from the csvprofiler directory.
Execute the CSV profiler by typing"
    java -jar csvprofiler/build/libs/csvprofiler-1.0-SNAPSHOT.jar -options.....

Alternatively, you can build a binary payload shell script for *nix (macos, linux, cygwin/gitbash/mingw)  by typing
    gradle clean makeShellScript
    
```shell
usage: java -jar <path-to-jar>
 -d <arg>    directory containing csv files, default: current directory
 -db <arg>   sqlite database (.db), file default: <current
             path>/csvprofiler.db
 -h          display this message
 -help       display this message
 -i <arg>    foreign key inclusion coefficient (0.0 - 1.0), default: 0.80
 -k <arg>    maximum unique key size (1 - 5), default: 2
 -kd <arg>   minimum key density non-null count/ total count, 0.0 - 1.0,
             defalt 1.0
 -l <arg>    limit number of lines to read and perform radom selection,
             default 100000
 -og         draw foreign key dependency graph
 -oj         output information schema as json
 -ox         output information schema as xml
 -s <arg>    sample coefficient (0.10 - 1.0), default: 1.0
 -vo         visualize foreign key graph only, from the specified database```
 
For example, if the directory /repos/grid-director/testData/cim contained several *.csv files you would type:

java -jar csvprofiler/build/libs/csvprofiler-1.0-SNAPSHOT.jar -d /repos/grid-director/testData/cim -db cim.db -k 2 -i 0.75 -s 0.25
or
./profiler.sh -d /repos/grid-director/testData/cim -db cim.db -k 2 -i 0.75 -oj > cim.json


This utility is for internal use only, not for distribution, because it uses the GPL licensed graphstream library for graph display.

