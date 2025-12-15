# Introduction
In this project I built a small Java command-line ?grep? app and wired it into a Maven/IntelliJ workflow. The app recursively scans files under a root directory, applies a user-supplied regex, and writes matching lines to an output file. I implemented `RegexExc` and `LambdaStreamExc` to practice core Java, regex, lambdas, and streams, then created `JavaGrepImp` and `JavaGrepLambdaImp` using SLF4J/Log4j for logging. The project is packaged with Maven Shade and dockerized using an OpenJDK image for easy distribution.

# Quick Start
Build the fat jar:

mvn clean package

Run from command line (args: `<regex> <rootPath> <outFile>`):

java -jar target/grep-*-SNAPSHOT.jar "ERROR.*" data/logs output/result.txt

Run the dockerized version with mounted input/output folders:

docker run --rm \
-v "$PWD/data":/data \
-v "$PWD/output":/output \
<docker_user>/grep "ERROR.*" /data /output/result.txt

# Implementation

## Pseudocode

process(pattern, rootPath, outFile):
files = listAllFiles(rootPath)          // walk directory tree

    matches = empty list

    for each file in files:
        for each line in readLines(file):   // buffered reader or Files.lines
            if line matches pattern:        // regex
                matches.add(format(file, line))

    writeLines(outFile, matches)            // create parent dirs if needed

`JavaGrepLambdaImp` keeps the same logic but replaces the explicit loops with `Files.walk` and `Files.lines` stream pipelines plus lambda filters and collectors.

## Performance Issue
The naïve version stores all matching lines in a `List` and only writes them at the end, which can use a lot of memory for huge inputs. To fix this, stream lines and write matches immediately (or in small batches), avoid holding large collections in memory, and use `Files.lines` with try-with-resources so file handles are closed promptly. For very large inputs we could also process file by file and optionally stream output directly to stdout.

# Test
I prepared a `data/` directory with several sample files: normal logs, empty files, large files, and files without matches. Then I ran the app with different regexes (simple words, patterns with special characters, and edge cases like `^$` or `.*`). For each run I manually inspected the output file and compared it against Unix `grep` or IntelliJ search results to verify both the number of matches and the exact matched lines.

# Deployment
The app is built as a shaded JAR using Maven Shade, so it has all dependencies in one artifact. I wrote a Dockerfile based on an OpenJDK 8 image, copied the shaded JAR into the image, and set `ENTRYPOINT ["java","-jar","app.jar"]`. Input and output directories are passed as mounted volumes (`/data` for input and `/output` for results), so users can run the same image on any machine with Docker without installing Java or Maven. The image `<docker_user>/grep` was built on a `feature/dockerize` branch, tested locally, and then pushed to Docker Hub.

# Improvement
1. Add automated tests with JUnit (including parameterized tests) for regex behavior, error handling, and edge cases, and wire them into Maven Surefire.
2. Improve the CLI by using a library such as picocli to provide `--help`, argument validation, better error messages, and configuration options (e.g., case-insensitive, file name only, colored output).
3. Enhance performance and usability for very large datasets by supporting parallel directory walks, streaming output directly to stdout, configurable logging/metrics, and better handling of I/O errors and permissions.
