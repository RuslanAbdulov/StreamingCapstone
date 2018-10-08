import java.util.regex.{MatchResult, Pattern}

object Regexper {
  def main(args: Array[String]): Unit = {
    var evPattern = """^\[?(\{.*\})[\,\]]?$"""
    var p = Pattern.compile(evPattern)
//    var p = "^\\[?(\\{.*\\})[\\,\\]]?$".r.pattern
    var m = p.matcher("{\"unix_time\": 1538932022, \"category_id\": 1009, \"ip\": \"172.10.2.2\", \"type\": \"view\"},")
    var found = m.find()
    var mr: MatchResult = m.toMatchResult
    var group = mr.group(1)
  }
}
