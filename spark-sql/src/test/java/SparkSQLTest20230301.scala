import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @author wangyingkang
 * @date 2023/3/1 16:23
 * @version 1.0
 * @Description SparkSQL基础测试题
 */
object SparkSQLTest20230301 {
  case class Student(studentId: Int, name: String, gender: String, birthday: String, classNo: String)

  case class Course(courseId: String, courseName: String, teacherId: Int)

  case class Score(studentId: Int, courseId: String, teacherId: Int)

  case class Teacher(teacherId: Int, teacherName: String, teacherSex: String, teacherBir: String, jobTitle: String, teacherDept: String)

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local[*]").appName("SparkSQLTest20230301").getOrCreate()
    val sparkContext = spark.sparkContext


    // 设置日志级别
    sparkContext.setLogLevel("WARN")

    //读取text文件
    val rddStudent: RDD[String] = sparkContext.textFile("input/txt/student.txt")
    val rddCourse: RDD[String] = sparkContext.textFile("input/txt/course.txt")
    val rddScore: RDD[String] = sparkContext.textFile("input/txt/score.txt")
    val rddTeacher: RDD[String] = sparkContext.textFile("input/txt/teacher.txt")

    //导入隐式转换
    import spark.implicits._

    //将文本行内容转为实体对象，再转为df
    rddStudent.map(line => {
      val fields = line.split("\t")
      Student(fields(0).toInt, fields(1), fields(2), fields(3), fields(4))
    }).toDF().createOrReplaceTempView("student")

    rddCourse.map(line => {
      val fields = line.split("\t")
      Course(fields(0), fields(1), fields(2).toInt)
    }).toDF().createOrReplaceTempView("course")

    rddScore.map(line => {
      val fields = line.split("\t")
      Score(fields(0).toInt, fields(1), fields(2).toInt)
    }).toDF().createOrReplaceTempView("score")

    rddTeacher.map(line => {
      val fields = line.split("\t")
      Teacher(fields(0).toInt, fields(1), fields(2), fields(3), fields(4), fields(5))
    }).toDF().createOrReplaceTempView("teacher")

    //1.查询Student表中“95031”班或性别为“女”的同学记录。
    spark.sql("select * from student where classNo='95031' or gender='女'").show()

  }
}
