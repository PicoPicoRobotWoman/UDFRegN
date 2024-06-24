
import org.apache.spark.sql.functions.udf

import java.sql.{Date, Timestamp}
import scala.reflect.runtime.universe
import scala.tools.reflect.ToolBox

object UDFReg1 extends App with Configure {

	val indexcode = Integer.parseInt(args(0))

	import Util.UDFHelper._
	val udfConfig = udfsConfigs(indexcode)
	val functionCode = udfConfig.configToString()

	val toolbox = universe.runtimeMirror(getClass.getClassLoader).mkToolBox()

	val tree = toolbox.parse(functionCode)
	val compiledCode = toolbox.compile(tree)
	val function = compiledCode().asInstanceOf[Function1[Any, Any]]

 	val myUDF = udfConfig.targetType match {
		case "String" => udf((value0: Any) => function(value0).asInstanceOf[String])
		case "Int" => udf((value0: Any) => function(value0).asInstanceOf[Int])
		case "Boolean" => udf((value0: Any) => function(value0).asInstanceOf[Boolean])
		case "Byte" => udf((value0: Any) => function(value0).asInstanceOf[Byte])
		case "Short" => udf((value0: Any) => function(value0).asInstanceOf[Short])
		case "Long" => udf((value0: Any) => function(value0).asInstanceOf[Long])
		case "Float" => udf((value0: Any) => function(value0).asInstanceOf[Float])
		case "Double" => udf((value0: Any) => function(value0).asInstanceOf[Double])
		case "Date" => udf((value0: Any) => function(value0).asInstanceOf[Date])
		case "Timestamp" => udf((value0: Any) => function(value0).asInstanceOf[Timestamp])
		case _ => throw new IllegalArgumentException(f"Неизвестный тип")
	}

	spark.udf.register(udfConfig.name, myUDF) 

}
