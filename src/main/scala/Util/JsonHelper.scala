package Util

import org.json4s.Formats
import org.json4s.native.JsonMethods.parse

object JsonHelper {

	private def jsonToListConvert[JsonType](json: String)(implicit mf: Manifest[JsonType], formats: Formats): List[Either[Throwable, JsonType]] = {
		parse(json)
			.extract[List[JsonType]]
			.map(Right(_))
	}

	implicit class JsonConverter(json: String) {

		def jsonToList[JsonType]()(implicit mf: Manifest[JsonType], formats: Formats): List[Either[Throwable, JsonType]]= {
			jsonToListConvert[JsonType](json)
		}

	}

}


