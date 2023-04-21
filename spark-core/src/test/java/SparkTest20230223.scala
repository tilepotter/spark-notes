import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author wangyingkang
 * @date 2023/2/23 11:10
 * @version 1.0
 * @Description Spark基础能力测试题
 */
object SparkTest20230223 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("spark-test-20230223")
    val sc = new SparkContext(sparkConf)

    //1.读取文件的数据test.txt
    val inputRDD: RDD[String] = sc.textFile("input/txt/test.txt")

    val mapRDD: RDD[Student] = inputRDD.map(line => {
      val fields = line.split(" ")
      Student(fields(0), fields(1), fields(2).toInt, fields(3), fields(4), fields(5).toInt)
    })

    // 2. 一共有多少个小于20岁的人参加考试？   2
    val count1: Long = mapRDD.filter(stu => stu.age < 20).groupBy(_.name).count()
    println("共有: " + count1 + " 个小于20岁的人参加考试")

    //3. 一共有多少个等于20岁的人参加考试？ 2
    val count2 = mapRDD.filter(stu => stu.age == 20).groupBy(_.name).count()
    println("共有: " + count2 + " 个等于20岁的人参加考试")

    val count3 = mapRDD.filter(stu => stu.age > 20).groupBy(_.name).count()
    println("共有: " + count3 + " 个大于20岁的人参加考试")

    //5. 一共有多个男生参加考试？
    //6. 一共有多少个女生参加考试？
    val count4 = mapRDD.filter(stu => stu.gender.equals("男")).groupBy(_.name).count()
    println("共有: " + count4 + " 个男生参加考试")

    val resultByGender: Array[(String, Int)] = mapRDD.groupBy(_.gender).map(x => (x._1, x._2.groupBy(_.name))).map(x => (x._1, x._2.size)).collect()
    println(resultByGender.mkString("Array(", ", ", ")"))

    //7. 12班有多少人参加考试？
    val count7 = mapRDD.filter(stu => stu.classNo.equals("12")).groupBy(_.name).count()
    println("12班共有" + count7 + "人参加考试")
    //8. 13班有多少人参加考试？
    val count8 = mapRDD.filter(stu => stu.classNo.equals("13")).groupBy(_.name).count()
    println("13班共有" + count8 + "人参加考试")
    val resultByClass: Array[(String, Int)] = mapRDD.groupBy(_.classNo).map(x => (x._1, x._2.groupBy(_.name))).map(x => (x._1, x._2.size)).collect()
    println(resultByClass.mkString("Array(", ", ", ")"))

    //    9. 语文科目的平均成绩是多少？
    val chineseAvg = mapRDD.filter(x => x.subject.equals("chinese")).map(x => x.score).mean()
    println("语文科目的平均成绩为：" + chineseAvg)
    //  10. 数学科目的平均成绩是多少？
    val mathAvg = mapRDD.filter(x => x.subject.equals("math")).map(x => x.score).mean()
    println("数学科目的平均成绩为：" + chineseAvg)
    //    11. 英语科目的平均成绩是多少？
    val englishAvg = mapRDD.filter(x => x.subject.equals("english")).map(stu => stu.score).mean()
    println("英语科目的平均成绩为：" + englishAvg)

    val subjectAvgScore: Array[(String, Int)] = mapRDD.map(stu => (stu.subject, stu.score)).groupByKey().map(x => (x._1, x._2.sum / x._2.size)).collect()
    println("每一科目的平均成绩为:" + subjectAvgScore.mkString("Array(", ", ", ")"))

    // 12. 每个人平均成绩是多少？
    val stuAvgScore: Array[(String, Int)] = mapRDD.map(stu => (stu.name, stu.score)).groupByKey().map(x => (x._1, x._2.sum / x._2.size)).collect()
    println("每个人的平均成绩为：" + stuAvgScore.mkString("Array(", ", ", ")"))

    // 13. 12班平均成绩是多少？
    // 16. 13班平均成绩是多少？
    val classAvgScore: Array[(String, Int)] = mapRDD.map(stu => (stu.classNo, stu.score)).groupByKey().map(x => (x._1, x._2.sum / x._2.size)).collect()
    println("每个班级的平均成绩：" + classAvgScore.mkString("Array(", ", ", ")"))

    //    14. 12班男生平均总成绩是多少？
    //    15. 12班女生平均总成绩是多少？
    //    17. 13班男生平均总成绩是多少？
    //    18. 13班女生平均总成绩是多少？
    val classGenderAvgScore: Array[((String, String), Int)] = mapRDD.map(stu => ((stu.classNo, stu.gender), stu.score)).groupByKey().map(x => (x._1, x._2.sum / x._2.size)).collect()
    println("每个班的男、女生平均成绩为：" + classGenderAvgScore.mkString("Array(", ", ", ")"))

    // 19. 全校语文成绩最高分是多少
    val maxChineseScore: Int = mapRDD.filter(stu => stu.subject.equals("chinese")).map(x => x.score).max()
    println("全校语文成绩最高分为：" + maxChineseScore)

    // 20. 12班语文成绩最低分是多少？
    val minChineseScoreOf12: Int = mapRDD.filter(stu => stu.classNo.equals("12") && stu.subject.equals("chinese")).map(x => x.score).min()
    println("12班语文成绩最低分为：" + minChineseScoreOf12)

    // 21. 13班数学最高成绩是多少？
    val maxMathScoreOf13 = mapRDD.filter(stu => stu.classNo.equals("13") && stu.subject.equals("math")).map(x => x.score).max()
    println("13班数学成绩最高分为：" + maxMathScoreOf13)

    // 22. 总成绩大于150分的12班的女生有几个？
    val result: Array[(String, Int)] = mapRDD.filter(stu => stu.classNo.equals("12") && stu.gender.equals("女")).map(x => (x.name, x.score)).groupByKey().map(x => (x._1, x._2.sum))
      .filter(x => x._2 > 150).collect()
    println("总成绩大于150分的12班的女生: " + result.mkString("Array(", ", ", ")"))

    // 总成绩大于150分，且数学大于等于70，且年龄大于等于19岁的学生的平均成绩是多少？
    val stag1: RDD[(String, Int)] = mapRDD
      .filter(stu => stu.age >= 19) //过滤年龄大于等于19岁
      .map(x => (x.name, x.score)) //转换为 (name,score) 形式
      .groupByKey() //按名字分组
      .filter(x => x._2.sum > 150) //过滤总成绩大于150分的
      .map(x => (x._1, x._2.sum / x._2.size)) //计算平均成绩==> (name,avgScore)


    val stag2: RDD[(String, Int)] = mapRDD.filter(stu => stu.age >= 19) //过滤年龄大于等于19岁
      .map(x => (x.name, x.subject, x.score)) //转换为 (name,subject,score) 形式
      .filter(x => x._2.equals("math") && x._3 >= 70) // 过滤数学大于等于70的
      .map(x => (x._1, x._3))

    val resultAll: Array[(String, Int)] = stag1.join(stag2) //结果为(王英,(73,80)), (杨春,(70,70))  （name,(平均成绩,数学成绩)）
      .map(x => (x._1, x._2._1)) //转换为 (name,平均成绩)
      .collect()
    println("总成绩大于150分，且数学大于等于70，且年龄大于等于19岁的学生的平均成绩为：" + resultAll.mkString("Array(", ", ", ")"))
  }
}
