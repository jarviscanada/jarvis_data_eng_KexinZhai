package ca.jrvs.apps.practice;

import java.util.regex.Pattern;

/**
 * Implementation of RegexExc using Java regex APIs.
 */
public class RegexExcImp implements RegexExc {

  // Match any name ending with .jpg or .jpeg (case insensitive)
  private static final Pattern JPEG_PATTERN =
      Pattern.compile(".+\\.(jpg|jpeg)$", Pattern.CASE_INSENSITIVE);

  // Simplified IP: four groups of 1?3 digits separated by dots
  // Range 0.0.0.0 to 999.999.999.999 (we don't check real IPv4 range here)
  private static final Pattern IP_PATTERN =
      Pattern.compile("^([0-9]{1,3}\\.){3}[0-9]{1,3}$");

  // Line is empty or only whitespace
  private static final Pattern EMPTY_LINE_PATTERN =
      Pattern.compile("^\\s*$");

  @Override
  public boolean matchJpeg(String filename) {
    if (filename == null) {
      return false;
    }
    return JPEG_PATTERN.matcher(filename).matches();
  }

  @Override
  public boolean matchIp(String ip) {
    if (ip == null) {
      return false;
    }
    return IP_PATTERN.matcher(ip).matches();
  }

  @Override
  public boolean isEmptyLine(String line) {
    if (line == null) {
      return false; // usually we don't treat null as "empty line"
    }
    return EMPTY_LINE_PATTERN.matcher(line).matches();
  }

  /**
   * Simple manual test.
   * You can right-click this main method in IntelliJ and Run it.
   */
  public static void main(String[] args) {
    RegexExc regex = new RegexExcImp();

    System.out.println("=== matchJpeg ===");
    System.out.println(regex.matchJpeg("pic.jpg"));     // true
    System.out.println(regex.matchJpeg("PIC.JPEG"));    // true
    System.out.println(regex.matchJpeg("pic.png"));     // false
    System.out.println(regex.matchJpeg(null));          // false

    System.out.println("\n=== matchIp ===");
    System.out.println(regex.matchIp("0.0.0.0"));          // true
    System.out.println(regex.matchIp("192.168.1.1"));      // true
    System.out.println(regex.matchIp("999.999.999.999"));  // true (allowed by spec)
    System.out.println(regex.matchIp("1.2.3"));            // false
    System.out.println(regex.matchIp("1.2.3.4.5"));        // false
    System.out.println(regex.matchIp(null));               // false

    System.out.println("\n=== isEmptyLine ===");
    System.out.println(regex.isEmptyLine(""));         // true
    System.out.println(regex.isEmptyLine("   "));      // true
    System.out.println(regex.isEmptyLine("\t  \n"));   // true
    System.out.println(regex.isEmptyLine(" abc "));    // false
    System.out.println(regex.isEmptyLine(null));       // false
  }
}
