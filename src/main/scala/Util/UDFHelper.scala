package Util

import config.UDFConfig

object UDFHelper {

	private def configToStringConvert(udfConfig: UDFConfig): String = {

		//создадим все импорты для итоговой строки
		val imports = udfConfig
			.imports 																 // достаем импорты из конфига
			.map(_.trim.stripPrefix("import ").trim) // удаляем с лева ключевое слово если оно есть
			.distinct 															 // оставляем уникальные
			.map(imp => f"import ${imp}")            // конкатенируем слева ключевое слово
			.mkString("\n")                          // все соеденяем через отступ

		val Array(params, functionBody) = udfConfig.fun.split("=>", 2).map(_.trim) // Отделяем функциональную часть от переменных

		val paramsTypes: Seq[(String, String)] = params
			.stripPrefix("(") 			// Убираем с лева скобку
			.stripSuffix(")") 			// Убираем с права скобку
			.split(",")       			// Разделяем параметры
			.map(_.trim)      			// Убираем лишние пробелы
			.map {
				case "" => null
				case param =>
					val Array(valueName, valueType) = param.split(":").map(_.trim)
					(valueName, valueType)
			}                 			// разделяем имя переменой от типа данных этой переменной
			.filter(_ != null) 			// отвильтровываем null-ы

		val funcTypes: String = paramsTypes.size match {
			case 0 => udfConfig.targetType
			case _ => f"${List
				.fill(paramsTypes.size)("Any")
				.mkString("", ", ", ", ")}${udfConfig.targetType}"
		} //здесь получаем перечисление через запятую типов данных Function

		val anyParams = paramsTypes.map(_._1.trim + "_any").mkString(", ") //парметры лямда вырожения

		val instances = paramsTypes.map {
			case (valueName, valueType) =>
				f"	val ${valueName}: ${valueType} = ${valueName}_any.asInstanceOf[${valueType}]"
		}.mkString("\n") // тут определяем конвертации парметров люмбды в необхадимые типы

		// собираем все вместе в итоговый стринг
		f"""
			 |${imports}
			 |
			 |val func: Function${paramsTypes.size}[${funcTypes}] = (${anyParams}) => {
			 |
			 |${instances}
			 |
			 |  (
			 |${functionBody}
			 |  ).asInstanceOf[${udfConfig.targetType}]
			 |
			 |}
			 |
			 |func
			 |""".stripMargin

	}

	implicit class Converter(udfConfig: UDFConfig) {

		def configToString(): String = {
			configToStringConvert(udfConfig)
		}

	}

}
