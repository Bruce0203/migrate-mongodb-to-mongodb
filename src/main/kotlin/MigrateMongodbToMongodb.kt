import com.mongodb.client.MongoClients
import java.io.File
import java.util.Properties

fun main() {
    val prop = Properties()
    prop.load(File("db.properties").inputStream())
    migrate(prop.getProperty("old")!!, prop.getProperty("new")!!)
}

fun migrate(oldName: String, newName: String) {
    val errors = ArrayList<String>()
    var newClient = MongoClients.create(newName)
    val oldClient = MongoClients.create(oldName)
    oldClient.listDatabaseNames().forEach { dbName ->
        val newDB = newClient.getDatabase(dbName)
        val oldDB = oldClient.getDatabase(dbName)
        oldDB.listCollectionNames().forEach { colName ->
            newDB.createCollection(colName)
            val newCol = newDB.getCollection(colName)
            val oldCol = oldDB.getCollection(colName)
            oldCol.find().forEach {
                println(it)
                try {
                    newCol.insertOne(it)
                } catch(e: Exception) {
                    newClient = MongoClients.create(newName)
                    e.stackTraceToString().apply { errors.add(it.toString() + this) }
                }
            }
        }
    }
    errors.forEach(::println)
    println("Done! ${errors.size} errors skipped!")
}
