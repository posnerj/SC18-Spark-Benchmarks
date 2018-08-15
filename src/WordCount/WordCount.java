package WordCount;

import java.io.BufferedReader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public final class WordCount {

  public static void main(String[] args) {
    if (args.length != 3) {
      System.out.println("Usage: WordCount <textfile> <repetitions> <partitions>");
      System.exit(1);
    }

    final String textFile = args[0];
    final int repetitions = Integer.parseInt(args[1]);
    final int partitions = Integer.parseInt(args[2]);


    final SparkConf sparkConf = new SparkConf().setAppName("WordCount");
    final JavaSparkContext jsc = new JavaSparkContext(sparkConf);

    final long before = System.nanoTime();

    StringBuilder builder = new StringBuilder();
    for (int i = 0; i < repetitions; ++i) {
      builder.append(textFile);
      if ((i + 1) < repetitions) {
        builder.append(",");
      }
    }
    final String files = builder.toString();

    JavaPairRDD<String, String> lines = jsc.wholeTextFiles(files, partitions);

    final JavaRDD<String> words = lines.flatMap(s -> {
      ArrayList<String> list = new ArrayList<>();
      StringReader stringReader = new StringReader(s._2());
      BufferedReader bufferedReader = new BufferedReader(stringReader);
      String line;
      while ((line = bufferedReader.readLine()) != null) {
        list.addAll(Arrays.asList(line.split(" ")));
      }
      return list.iterator();
    });

    final JavaPairRDD<String, Integer> ones = words.mapToPair(s -> new Tuple2<>(s, 1));
    final JavaPairRDD<String, Integer> counts = ones.reduceByKey((i1, i2) -> i1 + i2);
    final List<Tuple2<String, Integer>> output = counts.collect();

    long sum = 0;
    for (Tuple2<String, Integer> tuple : output) {
      System.out.println(tuple._1() + ": " + tuple._2());
      sum += tuple._2();
    }
    System.out.println("sum=" + sum);
    final long after = System.nanoTime();
    System.out.println("Times:" + ((after - before) / 1E9) + " sec");
  }
}
