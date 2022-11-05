import com.mongodb.client.MongoClients
import java.io.File
import java.util.Properties

fun main() {
    val prop = Properties()
    prop.load(File("db.properties").inputStream())
    migrate(prop.getProperty("old")!!, prop.getProperty("new")!!)
}

fun migrate(oldName: String, newName: String) {
    val newClient = MongoClients.create(newName)
    val oldClient = MongoClients.create(oldName)
    oldClient.listDatabaseNames().forEach { dbName ->
        val newDB = newClient.getDatabase(dbName)
        val oldDB = oldClient.getDatabase(dbName)
        oldDB.listCollectionNames().forEach { colName ->
            val newCol = newDB.getCollection(colName)
            val oldCol = oldDB.getCollection(colName)
            oldCol.find().forEach {
                newCol.insertOne(it)
                println(it)
            }
        }
    }
    println("Done!")
}
