package UTS;

import java.util.LinkedList;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public final class UTS {

  public static void main(String[] args) {
    if (args.length != 6) {
      System.out.println(
          "Usage: UTS <seed> <depth> <sequential_depth> <branching_factor> <totalCores> <tasksPerCore>");
      System.exit(1);
    }

    final int seed = Integer.parseInt(args[0]);
    final int depth = Integer.parseInt(args[1]);
    final int seqDepth = Integer.parseInt(args[2]);
    final int b = Integer.parseInt(args[3]);
    final int totalCores = Integer.parseInt(args[4]); // sum of all cores of all nodes
    final int tasksPerCore = Integer.parseInt(args[5]);
    final int totalTasks = totalCores * tasksPerCore;
    final double den = Math.log(b / (1.0 + b));

    final SparkConf sparkConf = new SparkConf().setAppName("UTS");
    final JavaSparkContext jsc = new JavaSparkContext(sparkConf);

    final long before = System.nanoTime();
    LinkedList<TreeNode> firstNodes = new LinkedList<>();
    TreeNode.push(new SHA1Rand(seed, depth), firstNodes, den);
    long count = TreeNode.processSequential(firstNodes, den, seqDepth) + 1;

    JavaRDD<TreeNode> nodes = jsc.parallelize(firstNodes, totalTasks);
    JavaRDD<Long> counts = nodes.map(node -> {
      LinkedList<TreeNode> newNodes = new LinkedList<>();
      newNodes.add(node);
      return TreeNode.processDFS(newNodes, den);
    });
    count += counts.reduce((x, y) -> x + y);

    System.out.println("Result: " + count);
    final long after = System.nanoTime();
    System.out.println("Times:" + ((after - before) / 1E9) + " sec");
  }
}
