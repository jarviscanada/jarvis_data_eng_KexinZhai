package ca.jrvs.apps.grep;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.log4j.BasicConfigurator;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

public class JavaGrepImp implements JavaGrep {

  // Logger (same as in the spoiler screenshot)
  final Logger logger = LoggerFactory.getLogger(JavaGrep.class);

  // CLI arguments stored as member variables (encapsulation)
  private String regex;
  private String rootPath;
  private String outFile;

  /**
   * Handle CLI arguments and start the app.
   *
   * USAGE: JavaGrep regex rootPath outFile
   */
  public static void main(String[] args) {
    if (args.length != 3) {
      throw new IllegalArgumentException("USAGE: JavaGrep regex rootPath outFile");
    }

    // Use log4j default configuration (same as spoiler screenshot)
    BasicConfigurator.configure();

    JavaGrepImp javaGrepImp = new JavaGrepImp();
    javaGrepImp.setRegex(args[0]);
    javaGrepImp.setRootPath(args[1]);
    javaGrepImp.setOutFile(args[2]);

    try {
      javaGrepImp.process();
    } catch (Exception ex) {
      javaGrepImp.logger.error("Error: Unable to process", ex);
    }
  }

  // ========= Implementation of JavaGrep interface =========

  /**
   * Follow the pseudocode from the assignment:
   *
   * matchedLines = []
   * for file in listFilesRecursively(rootDir)
   *   for line in readLines(file)
   *     if containsPattern(line)
   *       matchedLines.add(line)
   * writeToFile(matchedLines)
   */
  @Override
  public void process() throws IOException {
    List<File> files = listFiles(getRootPath());
    List<String> matchedLines = new ArrayList<>();

    for (File file : files) {
      for (String line : readLines(file)) {
        if (containsPattern(line)) {
          matchedLines.add(line);
        }
      }
    }

    writeToFile(matchedLines);
  }

  @Override
  public List<File> listFiles(String rootDir) {
    List<File> result = new ArrayList<>();
    File root = new File(rootDir);

    if (!root.isDirectory()) {
      throw new IllegalArgumentException("Not a directory: " + rootDir);
    }

    listFilesRecursive(root, result);
    return result;
  }

  // Recursively traverse a directory tree and collect all files
  private void listFilesRecursive(File dir, List<File> result) {
    File[] files = dir.listFiles();
    if (files == null) {
      logger.warn("Cannot list files in directory: {}", dir.getAbsolutePath());
      return;
    }

    for (File f : files) {
      if (f.isDirectory()) {
        listFilesRecursive(f, result);
      } else {
        result.add(f);
      }
    }
  }

  @Override
  public List<String> readLines(File inputFile) throws IOException {
    if (!inputFile.isFile()) {
      throw new IllegalArgumentException("Not a file: " + inputFile.getAbsolutePath());
    }

    List<String> lines = new ArrayList<>();
    try (BufferedReader reader = new BufferedReader(new FileReader(inputFile))) {
      String line;
      while ((line = reader.readLine()) != null) {
        lines.add(line);
      }
    }

    return lines;
  }

  @Override
  public boolean containsPattern(String line) {
    // Use regex to test if the line matches; find() = line contains the pattern
    return Pattern.compile(regex).matcher(line).find();
  }

  @Override
  public void writeToFile(List<String> lines) throws IOException {
    File out = new File(getOutFile());
    File parent = out.getParentFile();

    if (parent != null && !parent.exists() && !parent.mkdirs()) {
      throw new IOException("Cannot create output directory: " + parent.getAbsolutePath());
    }

    try (BufferedWriter writer = new BufferedWriter(new FileWriter(out))) {
      for (String line : lines) {
        writer.write(line);
        writer.newLine();
      }
    }

    logger.info("Wrote {} lines to {}", lines.size(), out.getAbsolutePath());
  }

  // ========= Getters & Setters (same structure as spoiler screenshot) =========

  @Override
  public String getRootPath() {
    return rootPath;
  }

  @Override
  public void setRootPath(String rootPath) {
    this.rootPath = rootPath;
  }

  @Override
  public String getRegex() {
    return regex;
  }

  @Override
  public void setRegex(String regex) {
    this.regex = regex;
  }

  @Override
  public String getOutFile() {
    return outFile;
  }

  @Override
  public void setOutFile(String outFile) {
    this.outFile = outFile;
  }
}
