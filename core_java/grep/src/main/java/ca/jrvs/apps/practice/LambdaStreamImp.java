package ca.jrvs.apps.practice;

import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class LambdaStreamImp implements LambdaStreamExc {

  /**
   * Create a String stream from a var-args array.
   */
  @Override
  public Stream<String> createStrStream(String... strings) {
    return Arrays.stream(strings);
  }

  /**
   * Convert all input strings to upper case.
   */
  @Override
  public Stream<String> toUpperCase(String... strings) {
    return createStrStream(strings)
        .map(String::toUpperCase);
  }

  /**
   * Filter strings that DO NOT contain the given pattern.
   * According to the interface comment:
   *   filter(stream, "a") should return a stream where no element contains "a".
   */
  @Override
  public Stream<String> filter(Stream<String> stringStream, String pattern) {
    return stringStream
        .filter(s -> !s.contains(pattern));
  }

  /**
   * Create an IntStream from an int array.
   */
  @Override
  public IntStream createIntStream(int[] arr) {
    return Arrays.stream(arr);
  }

  /**
   * Convert a generic Stream to a List.
   */
  @Override
  public <E> List<E> toList(Stream<E> stream) {
    return stream.collect(Collectors.toList());
  }

  /**
   * Convert an IntStream to a List of Integers.
   */
  @Override
  public List<Integer> toList(IntStream intStream) {
    return intStream
        .boxed()  // primitive int -> Integer
        .collect(Collectors.toList());
  }

  /**
   * Create an IntStream for the closed range [start, end].
   */
  @Override
  public IntStream createIntStream(int start, int end) {
    return IntStream.rangeClosed(start, end);
  }

  /**
   * Convert an IntStream to a DoubleStream of square roots.
   */
  @Override
  public DoubleStream squareRootIntStream(IntStream intStream) {
    return intStream
        .mapToDouble(Math::sqrt);
  }

  /**
   * Keep only odd numbers from the given IntStream.
   */
  @Override
  public IntStream getOdd(IntStream intStream) {
    return intStream
        .filter(i -> i % 2 != 0);
  }

  /**
   * Return a lambda printer that prints: prefix + message + suffix.
   */
  @Override
  public Consumer<String> getLambdaPrinter(String prefix, String suffix) {
    return msg -> System.out.println(prefix + msg + suffix);
  }

  /**
   * Print each message using the given printer.
   */
  @Override
  public void printMessages(String[] messages, Consumer<String> printer) {
    Arrays.stream(messages)
        .forEach(printer);
  }

  /**
   * Print all odd numbers from the given IntStream
   * using the provided printer.
   */
  @Override
  public void printOdd(IntStream intStream, Consumer<String> printer) {
    getOdd(intStream)
        .forEach(i -> printer.accept(String.valueOf(i)));
  }

  /**
   * Square each integer from a nested stream using flatMap.
   *
   * Input:  Stream<List<Integer>>
   * Output: Stream<Integer> where each element is squared.
   */
  @Override
  public Stream<Integer> flatNestedInt(Stream<List<Integer>> ints) {
    return ints
        .flatMap(list -> list.stream()
            .map(n -> n * n));
  }

  /**
   * Simple main method for quick manual testing.
   */
  public static void main(String[] args) {
    LambdaStreamExc l = new LambdaStreamImp();
    l.printMessages(new String[]{"a", "b"}, l.getLambdaPrinter("msg:", "!"));
  }
}
