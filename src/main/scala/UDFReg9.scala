
import org.apache.spark.sql.functions.udf

import java.sql.{Date, Timestamp}
import scala.reflect.runtime.universe
import scala.tools.reflect.ToolBox

object UDFReg9 extends App with Configure {

	val indexcode = Integer.parseInt(args(0))

	import Util.UDFHelper._
	val udfConfig = udfsConfigs(indexcode)
	val functionCode = udfConfig.configToString()

	val toolbox = universe.runtimeMirror(getClass.getClassLoader).mkToolBox()

	val tree = toolbox.parse(functionCode)
	val compiledCode = toolbox.compile(tree)
	val function = compiledCode().asInstanceOf[Function9[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]]

 	val myUDF = udfConfig.targetType match {
		case "String" => udf((value0: Any, value1: Any, value2: Any, value3: Any, value4: Any, value5: Any, value6: Any, value7: Any, value8: Any) => function(value0, value1, value2, value3, value4, value5, value6, value7, value8).asInstanceOf[String])
		case "Int" => udf((value0: Any, value1: Any, value2: Any, value3: Any, value4: Any, value5: Any, value6: Any, value7: Any, value8: Any) => function(value0, value1, value2, value3, value4, value5, value6, value7, value8).asInstanceOf[Int])
		case "Boolean" => udf((value0: Any, value1: Any, value2: Any, value3: Any, value4: Any, value5: Any, value6: Any, value7: Any, value8: Any) => function(value0, value1, value2, value3, value4, value5, value6, value7, value8).asInstanceOf[Boolean])
		case "Byte" => udf((value0: Any, value1: Any, value2: Any, value3: Any, value4: Any, value5: Any, value6: Any, value7: Any, value8: Any) => function(value0, value1, value2, value3, value4, value5, value6, value7, value8).asInstanceOf[Byte])
		case "Short" => udf((value0: Any, value1: Any, value2: Any, value3: Any, value4: Any, value5: Any, value6: Any, value7: Any, value8: Any) => function(value0, value1, value2, value3, value4, value5, value6, value7, value8).asInstanceOf[Short])
		case "Long" => udf((value0: Any, value1: Any, value2: Any, value3: Any, value4: Any, value5: Any, value6: Any, value7: Any, value8: Any) => function(value0, value1, value2, value3, value4, value5, value6, value7, value8).asInstanceOf[Long])
		case "Float" => udf((value0: Any, value1: Any, value2: Any, value3: Any, value4: Any, value5: Any, value6: Any, value7: Any, value8: Any) => function(value0, value1, value2, value3, value4, value5, value6, value7, value8).asInstanceOf[Float])
		case "Double" => udf((value0: Any, value1: Any, value2: Any, value3: Any, value4: Any, value5: Any, value6: Any, value7: Any, value8: Any) => function(value0, value1, value2, value3, value4, value5, value6, value7, value8).asInstanceOf[Double])
		case "Date" => udf((value0: Any, value1: Any, value2: Any, value3: Any, value4: Any, value5: Any, value6: Any, value7: Any, value8: Any) => function(value0, value1, value2, value3, value4, value5, value6, value7, value8).asInstanceOf[Date])
		case "Timestamp" => udf((value0: Any, value1: Any, value2: Any, value3: Any, value4: Any, value5: Any, value6: Any, value7: Any, value8: Any) => function(value0, value1, value2, value3, value4, value5, value6, value7, value8).asInstanceOf[Timestamp])
		case _ => throw new IllegalArgumentException(f"Неизвестный тип")
	}

	spark.udf.register(udfConfig.name, myUDF) 

}
