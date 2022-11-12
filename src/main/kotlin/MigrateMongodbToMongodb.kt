import com.mongodb.client.MongoClients
import org.bson.Document
import java.io.File
import java.util.Properties

fun main() {
    val prop = Properties()
    prop.load(File("db.properties").inputStream())
    migrate(prop.getProperty("old")!!, prop.getProperty("new")!!)
}

fun migrate(oldName: String, newName: String) {
    val errors = ArrayList<String>()
    val queue = ArrayList<Pair<Pair<String, String>, Document>>()
    var newClient = MongoClients.create(newName)
    var oldClient = MongoClients.create(oldName)
    oldClient.listDatabaseNames().forEach { dbName ->
        val oldDB = oldClient.getDatabase(dbName)
        oldDB.listCollectionNames().forEach { colName ->
            val newCol = newClient.getDatabase(dbName).getCollection(colName)
            val oldCol = oldDB.getCollection(colName)
            oldCol.find().forEach {
                try {
                    println(it)
                    newCol.insertOne(it)
                } catch(e: Exception) {
                    queue.add(Pair(Pair(dbName, colName), it))
                    e.stackTraceToString().apply { errors.add(it.toString() + this) }
                }
            }
        }
    }
    errors.forEach(::println)
    newClient = MongoClients.create(newName)
    oldClient = MongoClients.create(oldName)
    queue.forEach {
        newClient.getDatabase(it.first.first).getCollection(it.first.second).insertOne(it.second)
    }
    println("Done! ${errors.size} errors skipped!")
}
