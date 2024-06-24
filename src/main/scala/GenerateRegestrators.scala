import java.io.{File, PrintWriter}

object GenerateRegestrators extends App {

	(0 to 10).toList.map {

		num =>

			val types = (0 to num).toList.map(_ => "Any").mkString(", ")
			val lambdaVaues = (0 until  num).toList.map(n => f"value${n}: Any").mkString(", ")
			val functionValues = (0 until  num).toList.map(n => f"value${n}").mkString(", ")

			f"""
				 |import org.apache.spark.sql.functions.udf
				 |
				 |import java.sql.{Date, Timestamp}
				 |import scala.reflect.runtime.universe
				 |import scala.tools.reflect.ToolBox
				 |
				 |object UDFReg${num} extends App with Configure {
				 |
				 |	val indexcode = Integer.parseInt(args(0))
				 |
				 |	import Util.UDFHelper._
				 |	val udfConfig = udfsConfigs(indexcode)
				 |	val functionCode = udfConfig.configToString()
				 |
				 |	val toolbox = universe.runtimeMirror(getClass.getClassLoader).mkToolBox()
				 |
				 |	val tree = toolbox.parse(functionCode)
				 |	val compiledCode = toolbox.compile(tree)
				 |	val function = compiledCode().asInstanceOf[Function${num}[${types}]]
				 |
				 | 	val myUDF = udfConfig.targetType match {
				 |		case "String" => udf((${lambdaVaues}) => function(${functionValues}).asInstanceOf[String])
				 |		case "Int" => udf((${lambdaVaues}) => function(${functionValues}).asInstanceOf[Int])
				 |		case "Boolean" => udf((${lambdaVaues}) => function(${functionValues}).asInstanceOf[Boolean])
				 |		case "Byte" => udf((${lambdaVaues}) => function(${functionValues}).asInstanceOf[Byte])
				 |		case "Short" => udf((${lambdaVaues}) => function(${functionValues}).asInstanceOf[Short])
				 |		case "Long" => udf((${lambdaVaues}) => function(${functionValues}).asInstanceOf[Long])
				 |		case "Float" => udf((${lambdaVaues}) => function(${functionValues}).asInstanceOf[Float])
				 |		case "Double" => udf((${lambdaVaues}) => function(${functionValues}).asInstanceOf[Double])
				 |		case "Date" => udf((${lambdaVaues}) => function(${functionValues}).asInstanceOf[Date])
				 |		case "Timestamp" => udf((${lambdaVaues}) => function(${functionValues}).asInstanceOf[Timestamp])
				 |		case _ => throw new IllegalArgumentException(f"Неизвестный тип")
				 |	}
				 |
				 |	spark.udf.register(udfConfig.name, myUDF)
				 |
				 |}
				 |""".stripMargin


	}
		.zipWithIndex
		.foreach {
			case (str, index) =>
				val file = new File(f"./src/main/scala/UDFReg${index}.scala")
				val writer = new PrintWriter(file)

				try {
					writer.write(str)
				} finally {
					writer.close()
				}
		}


}


