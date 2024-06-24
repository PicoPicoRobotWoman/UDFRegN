import org.apache.spark.sql.functions.udf

object Example1 extends App with Configure {

	spark.udf.register("blue", udf(() => "0000FF")) // создаем и регестрируем udf
	spark.sql("select blue()").show()
	/* вывод:
			+------+
			|blue()|
			+------+
			|0000FF|
			+------+
	 */

}
