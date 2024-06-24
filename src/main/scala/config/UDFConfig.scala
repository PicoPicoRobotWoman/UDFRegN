package config

case class UDFConfig(name: String,
										 targetType: String,
										 fun: String,
										 imports: List[String] = List.empty)
