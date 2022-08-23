package CloudCoumputing

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, explode, from_json}
import org.apache.spark.sql.types.{ArrayType, IntegerType, StringType, StructField, StructType}
import org.json4s.jackson.JsonMethods.{compact, render}
import org.json4s.JsonDSL._
import java.io.Serializable



object Main{
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .appName("DataAnalyst").config("spark.jars","/D:/云计算课程设计/out/artifacts/_jar/云计算课程设计.jar")
      .master("spark://192.168.75.142:7077").getOrCreate()

    val schemaString = "budget,genres,homepage,id,keywords,original_language,original_title,overview," +
      "popularity,production_companies,production_countries," +
      "release_date,revenue,runtime,spoken_languages,status," + "tagline,title,vote_average,vote_count"
    //将上面的字符串拆分出来 并且map到每一个StructField对象里面
    val fields = schemaString.split(",").map(fieldName =>
      StructField(fieldName, StringType, nullable = true))
    //创建StructType对象，该对象包含每个字段的名称
    val schema = StructType(fields)


    //创建mdf对象
    val mdf = spark.read.format("csv").schema(schema)
      //指定所谓的表头
      .option("inferSchema",value = false).option("header",value = true)
      //schema就是header
      .option("nullValue","\\N").option("escape","\"")
      .option("quoteAll","true").option("seq",",")
      .csv("hdfs://192.168.75.142:9000/user/hadoop/movies.csv")



    //保存成json文件
    def countByJson(field:String):org.apache.spark.rdd.RDD[(String,Int)]  ={
      val jsonSchema =ArrayType(new StructType().add("id", IntegerType).add("name",StringType))
      mdf.select(mdf.col(field)).filter(mdf.col(field).isNotNull)
        //     此处是单条中包含多个数据的json，按照jsonSchema的格式进行解析，并生成多条单个数据，explode是将数组组生成为列。
        .select(explode(from_json(mdf.col(field), jsonSchema)).as(field))
        //     解决$"genres.name"的变量问题
        .select(field.concat(".name")).rdd.map(name=>(name.toString(),1))
        .repartition(1).reduceByKey((x,y) => x + y)
    }
    //  保存文件
    def save(path:String, data:Serializable): Unit={
      val file =spark.sparkContext.parallelize(List(data))
      file.saveAsTextFile(path)
    }

    // 2. 前 100 个常见关键词
    def countByKeywords():String={
      //     对rdd排序
      val keywordsRDD=countByJson("keywords").sortBy(x=>(-x._2))
      val jsonString =keywordsRDD.take(100).toList.map { case(keywords,count) =>
        (("keywords" ->keywords.replace("[","").replace("]",""))~("count" ->count))
      }
      val mdfJson=compact(render(jsonString))
      mdfJson
    }


    def countByGenres():String={
      val genresRDD=countByJson("genres")
      val jsonString =genresRDD.collect().toList.map { case(genre,count) =>
        (("genre" ->genre.replace("[","").replace("]",""))~("count" ->count))
      }
      val mdfJson=compact(render(jsonString))
      mdfJson
    }


    def moviesOfRuntime(order:String,ascending:Boolean):Array[String]={
      //     后一个filter之前是dataset有两列一列runtime，一列为count
      mdf.groupBy("runtime").count().filter("count>=100").toJSON.collect()

    }
//    val movie_runtimeOfCountOver100 = new StringBuilder
//    movie_runtimeOfCountOver100 ++= "["
//    for (v <- runtimeOfCountOver100Arr){
//      movie_runtimeOfCountOver100 ++= v
//    }
//    movie_runtimeOfCountOver100 ++= "]"
//    movie_runtimeOfCountOver100.toString
//    val runtimeOfCountOver100Arr=moviesOfRuntime("count",ascending = false)


//    println(moviesOfRuntime("count",false).mkString("Array(", ", ", ")"))

    //  电影最多使用的10种语言
    def countByLanguageRDD():String={
      val countByLanguageRDD=countByJson("spoken_languages").sortBy(x=>(-x._2))
      val jsonString =countByLanguageRDD.take(10).toList.map { case(language,count) =>
        (("language" ->language.replace("[","").replace("]",""))~("count" ->count))
      }
      val mdfJson=compact(render(jsonString))
      mdfJson
    }
//    val movie_countByLanguage=countByLanguageRDD()
//    println(movie_countByLanguage)

    // 发行时间与评价的关系
    def dateVote(): Array[ String] = {
      mdf.select(mdf.col("release_date"), mdf.col("title"),
        mdf.col("vote_average")).toJSON.collect()
    }

//    val dateVoteArr=dateVote()
//    println(dateVoteArr.mkString("Array(", ", ", ")"))
//    val dateVoteSB = new StringBuilder
//    dateVoteSB ++= "["
//    for (v <- dateVoteArr){
//      dateVoteSB ++= v
//    }
//    dateVoteSB ++= "]"
////    println(dateVoteSB.toString)


    //  流行度和评价的关系
    def popVote():Array[String]={
      mdf.select("title","popularity","vote_average").filter(mdf.col("vote_count")>100).toJSON.collect()
    }
//    val popVoteArr=popVote()
//    println(popVote().mkString("Array(", ", ", ")"))
//    val popVoteSB = new StringBuilder
//    popVoteSB ++= "["
//    for (v <- popVoteArr){
//      popVoteSB ++= v
//    }
//    popVoteSB ++= "]"
//    println(popVoteSB.toString)

    //执行函数
    val wordCloud=countByKeywords()
    val genres = countByGenres()
    val moviesTime =moviesOfRuntime("count",false)
    val language = countByLanguageRDD()
    val dV = dateVote()
    val pV = popVote()

    val wordPath= "hdfs://192.168.75.142:9000/user/hadoop/output/wordCloud.json"   //保存路径
    val genresPath= "hdfs://192.168.75.142:9000/user/hadoop/output/genres.json"
    val timePath= "hdfs://192.168.75.142:9000/user/hadoop/output/moviesTime.json"
    val languagePath= "hdfs://192.168.75.142:9000/user/hadoop/output/language.json"
    val dVPath= "hdfs://192.168.75.142:9000/user/hadoop/output/dateVote.json"
    val pVPath= "hdfs://192.168.75.142:9000/user/hadoop/output/popVote.json"
    val data = List(wordCloud,genres,moviesTime,language,dV,pV)

    val Path = List(wordPath,genresPath,timePath,languagePath,dVPath,pVPath)
    for (i <- 0 to 5){
      save(Path.apply(i),data.apply(i))
    }

    println("所有结果已经保存在hdfs://master:9000/user/hadoop/output文件夹中!")

  }
}
