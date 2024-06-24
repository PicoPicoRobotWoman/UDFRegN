import org.apache.spark.sql.functions.udf

import scala.reflect.runtime.universe
import scala.tools.reflect.ToolBox

object Example3 extends App with Configure {

	val labdaCode = "() => \"0000FF\""

	import universe._
	val toolbox = universe.runtimeMirror(this.getClass.getClassLoader).mkToolBox()

	val tree = toolbox.parse(labdaCode)
	val compiledCode = toolbox.compile(tree)
	val function = compiledCode().asInstanceOf[Function0[String]]
	//сторока успешно конвертируеться в Function0(String)


	println(function()) // вывод: 0000FF
	val udfBlue = udf(() => function()) // успешное создание udf

	spark.udf.register("blue", udfBlue) // успешное регестрирование udf

	spark.sql("select blue()").show()
	/*вывод:

		+------+
		|blue()|
		+------+
		|0000FF|
		+------+

	 */

}
