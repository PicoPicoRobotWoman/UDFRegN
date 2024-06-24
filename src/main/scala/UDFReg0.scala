
import org.apache.spark.sql.functions.udf

import java.sql.{Date, Timestamp}
import scala.reflect.runtime.universe
import scala.tools.reflect.ToolBox

object UDFReg0 extends App with Configure {

	val indexcode = Integer.parseInt(args(0))

	import Util.UDFHelper._
	val udfConfig = udfsConfigs(indexcode)
	val functionCode = udfConfig.configToString()

	val toolbox = universe.runtimeMirror(getClass.getClassLoader).mkToolBox()

	val tree = toolbox.parse(functionCode)
	val compiledCode = toolbox.compile(tree)
	val function = compiledCode().asInstanceOf[Function0[Any]]

 	val myUDF = udfConfig.targetType match {
		case "String" => udf(() => function().asInstanceOf[String])
		case "Int" => udf(() => function().asInstanceOf[Int])
		case "Boolean" => udf(() => function().asInstanceOf[Boolean])
		case "Byte" => udf(() => function().asInstanceOf[Byte])
		case "Short" => udf(() => function().asInstanceOf[Short])
		case "Long" => udf(() => function().asInstanceOf[Long])
		case "Float" => udf(() => function().asInstanceOf[Float])
		case "Double" => udf(() => function().asInstanceOf[Double])
		case "Date" => udf(() => function().asInstanceOf[Date])
		case "Timestamp" => udf(() => function().asInstanceOf[Timestamp])
		case _ => throw new IllegalArgumentException(f"Неизвестный тип")
	}

	spark.udf.register(udfConfig.name, myUDF) 

}
