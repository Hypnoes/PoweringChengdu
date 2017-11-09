package project.utils
{   
    import scala.language.implicitConversions

    trait Jobs {
        def run(input: String, ouput: String): Unit
    }
    
    object Convert {
        implicit def toDate(stringDate: String): java.sql.Date = {
            val sdf = new java.text.SimpleDateFormat("yyyy/MM/dd")    
            return new java.sql.Date(sdf.parse(stringDate).getTime())
        }
    }
}
