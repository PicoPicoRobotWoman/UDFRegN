import org.apache.spark.sql.functions.udf

import scala.reflect.runtime.universe
import scala.tools.reflect.ToolBox

object Example2 extends App with Configure {

	def udfRegister(labdaCode: String): Unit = {

		import universe._
		val toolbox = universe.runtimeMirror(this.getClass.getClassLoader).mkToolBox()

		val tree = toolbox.parse(labdaCode)
		val compiledCode = toolbox.compile(tree)
		val function = compiledCode().asInstanceOf[Function0[String]]
		//сторока успешно конвертируеться в Function0(String)


		println(function()) // вывод: 0000FF
		val udfBlue = udf(() => function()) // успешное создание udf


		spark.udf.register("blue", udfBlue)

	}

	udfRegister("() => \"0000FF\"")
	spark.sql("select blue()").show() // возникает ошибка

}


