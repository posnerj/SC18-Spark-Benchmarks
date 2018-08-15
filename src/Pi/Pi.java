package Pi;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public final class Pi {

  public static void main(String[] args) {
    if (args.length != 3) {
      System.out.println("Usage: Pi <totalCores> <N> <tasksPerCore>");
      System.exit(1);
    }

    final int totalCores = Integer.parseInt(args[0]);
    final int N = Integer.parseInt(args[1]);
    final int tasksPerCore = Integer.parseInt(args[2]);
    final int totalTasks = totalCores * tasksPerCore;
    final long POINTS = 1L << N; // POINTS = 2^n
    final long pointsPerWorker = POINTS / totalTasks;

    final SparkConf sparkConf = new SparkConf().setAppName("Pi");
    final JavaSparkContext jsc = new JavaSparkContext(sparkConf);

    final long before = System.nanoTime();
    List<Integer> l = new ArrayList<>(totalTasks);
    for (int i = 0; i < totalTasks; i++) {
      l.add(i);
    }

    JavaRDD<Integer> dataSet = jsc.parallelize(l, totalTasks);

    long count = dataSet.map(integer -> {
      long tmpCount = 0;
      for (long i = 0; i < pointsPerWorker; ++i) {
        final double x = 2 * ThreadLocalRandom.current().nextDouble() - 1.0;
        final double y = 2 * ThreadLocalRandom.current().nextDouble() - 1.0;
        tmpCount += (x * x + y * y <= 1) ? 1 : 0;
      }
      return tmpCount;
    }).reduce((integer, integer2) -> integer + integer2);

    System.out.println("Pi is roughly " + 4.0 * count / POINTS);
    final long after = System.nanoTime();
    System.out.println("Times:" + ((after - before) / 1E9) + " sec");
  }
}
