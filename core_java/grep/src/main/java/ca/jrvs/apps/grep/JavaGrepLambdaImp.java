package ca.jrvs.apps.grep;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A grep implementation that uses Java 8 Lambdas and Stream APIs.
 *
 * This class reuses the overall workflow defined in {@link JavaGrepImp}
 * (i.e., {@code process()}), but overrides the I/O related methods
 * to use lazy streams instead of explicit loops and recursion.
 */
public class JavaGrepLambdaImp extends JavaGrepImp {

  /**
   * Program entry point.
   *
   * Expected arguments:
   *   args[0] = regex pattern
   *   args[1] = root directory to search
   *   args[2] = output file path
   */
  public static void main(String[] args) {

    if (args.length != 3) {
      throw new IllegalArgumentException("USAGE: regex rootPath outFile");
    }

    // Creating JavaGrepLambdaImp instead of JavaGrepImp
    // JavaGrepLambdaImp inherits all methods except two overridden methods in this class
    JavaGrepLambdaImp javaGrepLambdaImp = new JavaGrepLambdaImp();
    javaGrepLambdaImp.setRegex(args[0]);
    javaGrepLambdaImp.setRootPath(args[1]);
    javaGrepLambdaImp.setOutFile(args[2]);

    try {
      // Calling parent method (process),
      // but it will invoke the overridden methods in this class
      javaGrepLambdaImp.process();
    } catch (Exception ex) {
      javaGrepLambdaImp.logger.error("Error executing JavaGrepLambdaImp", ex);
    }
  }

  /**
   * Implement using lambda and stream APIs.
   *
   * Read all lines from the given input file and return them as a List.
   * This implementation uses {@link Files#lines(java.nio.file.Path)} to create
   * a lazily-evaluated {@link Stream} of lines, and then collects them into a List.
   */
  @Override
  public List<String> readLines(File inputFile) {
    try (Stream<String> lines = Files.lines(inputFile.toPath())) {
      return lines.collect(Collectors.toList());
    } catch (IOException e) {
      // Wrap checked exception in a RuntimeException so that the method
      // can keep the same signature as defined in the interface.
      throw new RuntimeException("Failed to read lines from file: " + inputFile.getPath(), e);
    }
  }

  /**
   * Implement using lambda and stream APIs.
   *
   * Recursively list all files under the given root directory.
   * This implementation uses {@link java.nio.file.Files#walk}
   * to traverse the directory tree as a {@link java.util.stream.Stream}
   * of {@link java.nio.file.Path} objects, filters out only regular files,
   * and converts them to {@link java.io.File} instances.
   */


  @Override
  public List<File> listFiles(String rootDir) {
    try (Stream<Path> paths = Files.walk(Paths.get(rootDir))) {
      return paths
          .filter(Files::isRegularFile)
          .map(Path::toFile)
          .collect(Collectors.toList());
    } catch (IOException e) {
      throw new RuntimeException("Failed to list files in directory: " + rootDir, e);
    }
  }
}
