object Example5 extends App with Configure {

	import Util.UDFHelper._
	udfsConfigs.zipWithIndex.foreach {
		case (udfConfig, index) =>
			udfConfig.fun.split("=>", 2)(0).count(_ == ':') match {
				case 0 => UDFReg0.main(Array( index.toString ))
				case 1 => UDFReg1.main(Array( index.toString ))
				case 2 => UDFReg2.main(Array( index.toString ))
				case 3 => UDFReg3.main(Array( index.toString ))
				case 4 => UDFReg4.main(Array( index.toString ))
				case 5 => UDFReg5.main(Array( index.toString ))
				case 6 => UDFReg6.main(Array( index.toString ))
				case 7 => UDFReg7.main(Array( index.toString ))
				case 8 => UDFReg8.main(Array( index.toString ))
				case 9 => UDFReg9.main(Array( index.toString ))
				case 10 => UDFReg10.main(Array( index.toString ))
				case _ => Unit
			}

	}

	spark.sql("select blue(), reverse('namoW tobor ociP ociP') AS rev, plus(1, 2), sum3(1, 1, 1)").show
	/* вывод

		+------+--------------------+----------+-------------+
		|blue()|                 rev|plus(1, 2)|sum3(1, 1, 1)|
		+------+--------------------+----------+-------------+
		|00ff00|Pico Pico robot W...|         3|            3|
		+------+--------------------+----------+-------------+

	 */


}
