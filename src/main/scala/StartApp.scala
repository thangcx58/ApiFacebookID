import akka.http.scaladsl.server._
import akka.http.scaladsl.model._
import akka.http.scaladsl.Http
import akka.actor.ActorSystem
import akka.http.scaladsl.model.headers.{Authorization, BasicHttpCredentials}
import akka.stream.ActorMaterializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql._
import play.api.libs.json.{JsString, JsValue, Json}
import com.datastax.spark.connector.cql.CassandraConnector
import java.util.UUID


object Server extends App with Directives {
    val config = ConfigFactory.load()

    val conf = new SparkConf()
        .setMaster("local[*]")
        .setAppName("api")
        .set("spark.cassandra.connection.host", config.getString("server.db_host"))
        .set("spark.cassandra.auth.username", config.getString("server.db_user"))
        .set("spark.cassandra.auth.password", config.getString("server.db_password"))

    val spark = SparkSession.builder().config(conf).getOrCreate()

    val sc = spark.sparkContext

    val keyspace = config.getString("server.db_keyspace")
    val table = config.getString("server.db_table")

    def getParam(df: DataFrame, colName: String) = {
        if (df.columns.contains(colName))
            df.select(colName).first().getString(0)
        else
            ""
    }
    /**
      * describe : controll request RESTFUL : domain/v1/fbid
      * params: params: input data is string type
      * return: if request includes field "fb_id", save data to database and return {receive:ok}
      *         others case: return {error: bad request}
      * */
    def process(params: String): HttpResponse = {

        val jsonObject = Json.parse(params)

        val id = jsonObject \ "fb_id"

        if (id.isEmpty) {
            val errorJson = "{\"error\":\" bad request\"}"
            HttpResponse(StatusCodes.InternalServerError, entity = HttpEntity(ContentTypes.`application/json`,errorJson))
        } else {
            val respJson = "{\"received\":\"OK\"}"
            val id_ = UUID.randomUUID().toString
            println(id_)
            saveData(jsonObject,id_)
            HttpResponse(StatusCodes.OK, entity = HttpEntity(ContentTypes.`application/json`, respJson))
        }
    }

    /**
      *  save data to database
      * @param data: input {fb_id: id}
      * @param id: id UUID
      */
    def saveData(data: JsValue, id:String): Unit = {

        val value = (data \ "fb_id").as[JsString].value
        val str: String =
            s"""
              |{
              | "id": "$id",
              | "fb_id": "$value"
              |}
            """.stripMargin
        val dataJson = Json.parse(str)
        print(dataJson)
        //check connect to Cassandra
        val connector = CassandraConnector(sc.getConf)
        connector.withSessionDo { session =>
            //save data to cassandra
            session.execute(s"use $keyspace")
            session.execute(s"INSERT INTO $table JSON '$dataJson' ")
        }

    }
    implicit val system = ActorSystem("actor-system")
    implicit val materializer = ActorMaterializer()

    val authHeader = Authorization(BasicHttpCredentials(config.getString("server.token")))

    val routes =
        path("v1" / "fbid") {
            post {
                extractRequest { request =>
                    if (scala.util.Try(request.getHeader("Authorization").get).isSuccess &&
                        authHeader.value() == request.getHeader("Authorization").get().value()) {
                        entity(as[String]) { params => complete(process(params)) }
                    }
                    else {
                        complete(HttpResponse(StatusCodes.Unauthorized, entity = "Invalid token"))
                    }
                }
            }
        }

    Http().bindAndHandle(routes, "0.0.0.0", config.getInt("server.port"))
}
