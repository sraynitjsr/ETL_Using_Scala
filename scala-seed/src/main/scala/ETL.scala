import java.io.File
import com.github.tototoshi.csv._

object ETL {

  case class Person(name: String, age: Int, city: String)

  def extract(filePath: String): List[Person] = {
    val reader = CSVReader.open(new File(filePath))
    val data = reader.allWithHeaders().map { row =>
      Person(row("name"), row("age").toInt, row("city"))
    }
    reader.close()
    data
  }

  def transform(data: List[Person]): List[Person] = {
    data.map(person => person.copy(age = person.age + 1)) // Increment age by 1
  }

  def main(args: Array[String]): Unit = {
    val extractedData = extract("data/input.csv")
    println("Extracted Data:")
    extractedData.foreach(println) // Print each extracted person

    val transformedData = transform(extractedData)
    println("\nTransformed Data:")
    transformedData.foreach(println) // Print each transformed person
  }
}
