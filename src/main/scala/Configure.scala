import config.UDFConfig
import org.apache.spark.sql.SparkSession
import org.json4s.DefaultFormats

import scala.io.Source

trait Configure {

	lazy val spark = SparkSession
		.builder()
		.appName("Example UDF runtime compile")
		.master("local[*]")
		.getOrCreate()
	lazy val sc = spark.sparkContext
	// создали спрак сессию и контекст

	lazy val pathUdfsJson = "./UDFs.json" //путь до json c функциями

	implicit lazy val formats = DefaultFormats // список форматов для json4s, в моем случае хватеает дефолтных форматов
	import Util.JsonHelper._

	lazy val udfJson = getJson(pathUdfsJson) // получаем сам json в виде строки
	lazy val udfsConfigs: List[UDFConfig] = udfJson
		.jsonToList[UDFConfig]
		.filter(_.isRight)
		.map {
			case Right(udfconfig) => udfconfig
		} // получаем все корректные udfconfig
			// этот обект мне и понадобиться

	//метод для чтения файлов
	private def getJson(path: String): String = {
		val file = Source.fromFile(path)
		try {
			file.getLines().mkString("\n")
		} finally {
			file.close()
		}
	}

}
