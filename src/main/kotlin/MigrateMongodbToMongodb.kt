import com.mongodb.client.MongoClients
import java.io.File
import java.util.Properties

fun main() {
    val prop = Properties()
    prop.load(File("db.properties").inputStream())
    migrate(prop.getProperty("old")!!, prop.getProperty("new")!!)
}

const val refreshPeriod = 20000
fun migrate(oldName: String, newName: String) {
    val errors = ArrayList<String>()
    val newClient = MongoClients.create(newName)
    val oldClient = MongoClients.create(oldName)
    var i = 0
    var lastI = 0
    while(true) {
        try {
            oldClient.listDatabaseNames().forEach { dbName ->
                val newDB = newClient.getDatabase(dbName)
                val oldDB = oldClient.getDatabase(dbName)
                oldDB.listCollectionNames().forEach { colName ->
                    newDB.createCollection(colName)
                    val newCol = newDB.getCollection(colName)
                    val oldCol = oldDB.getCollection(colName)
                    oldCol.find().forEach {
                        if (i < lastI) {
                            i++
                            println("skip-1 debug: $i, $lastI, $refreshPeriod")
                            return@forEach
                        } else if (i >= refreshPeriod) {
                            lastI += --i
                            i = 0
                            println("skip-2 debug: $i, $lastI, $refreshPeriod")
                            throw AssertionError()
                        }
                        i++
                        try {
                            println(it)
                            newCol.insertOne(it)
                        } catch(e: Exception) {
                            e.stackTraceToString().apply { errors.add(it.toString() + this) }
                        }
                    }
                }
            }
            errors.forEach(::println)
            println("Done! ${errors.size} errors skipped!")
        } catch(_: Exception) {}
    }
}
