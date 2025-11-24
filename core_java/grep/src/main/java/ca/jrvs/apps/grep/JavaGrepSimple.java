package ca.jrvs.apps.grep;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class JavaGrepSimple {

  private final Pattern pattern;
  private final File rootDir;
  private final File outFile;

  public JavaGrepSimple(String regex, String rootPath, String outPath) {
    this.pattern = Pattern.compile(regex);
    this.rootDir = new File(rootPath);
    this.outFile = new File(outPath);
  }

  public static void main(String[] args) {
    // Check command-line arguments
    if (args.length != 3) {
      System.err.println("USAGE: regex rootPath outFile");
      System.err.println("Example: \".*Romeo.*Juliet.*\" ./data ./out/result.txt");
      System.exit(1);
    }

    String regex = args[0];
    String rootPath = args[1];
    String outPath = args[2];

    JavaGrepSimple app = new JavaGrepSimple(regex, rootPath, outPath);
    try {
      app.process();
    } catch (IOException e) {
      System.err.println("Error running grep: " + e.getMessage());
      e.printStackTrace();
      System.exit(1);
    }
  }

  /**
   * Main entry point: validate paths, create output file,
   * then recursively search rootDir and write matches to outFile.
   */
  public void process() throws IOException {
    if (!rootDir.exists() || !rootDir.isDirectory()) {
      throw new IllegalArgumentException("rootPath is not a valid directory: " + rootDir.getAbsolutePath());
    }

    // Ensure the output directory exists
    File parent = outFile.getParentFile();
    if (parent != null && !parent.exists()) {
      if (!parent.mkdirs()) {
        throw new IOException("Failed to create output directory: " + parent.getAbsolutePath());
      }
    }

    try (BufferedWriter writer = new BufferedWriter(new FileWriter(outFile))) {
      searchDirectory(rootDir, writer);
    }
  }

  /**
   * Recursively traverse the directory tree.
   */
  private void searchDirectory(File dir, BufferedWriter writer) throws IOException {
    File[] files = dir.listFiles();
    if (files == null) {
      // Could be no permission or not a normal directory
      return;
    }

    for (File f : files) {
      if (f.isDirectory()) {
        // Recurse into subdirectory
        searchDirectory(f, writer);
      } else {
        // Regular file
        searchFile(f, writer);
      }
    }
  }

  /**
   * Read a file line by line, test against the regex,
   * and write matching lines to the output file.
   */
  private void searchFile(File file, BufferedWriter writer) throws IOException {
    try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
      String line;
      while ((line = reader.readLine()) != null) {
        if (matches(line)) {
          writer.write(line);
          writer.newLine();
        }
      }
    }
  }

  /**
   * Check whether a line matches the regex pattern.
   */
  private boolean matches(String line) {
    Matcher matcher = pattern.matcher(line);
    return matcher.find(); // match if the pattern appears anywhere in the line
  }
}
