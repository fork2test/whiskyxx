package io.vertx.blog.first.service;

import io.vertx.blog.first.model.Whisky;
import io.vertx.blog.first.repository.WhiskyRepository;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.http.HttpServer;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.SQLConnection;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.StaticHandler;

import java.util.List;
import java.util.stream.Collectors;

/**
 * This is a verticle. A verticle is a _Vert.x component_. This verticle is implemented in Java, but you can
 * implement them in JavaScript, Groovy or even Ruby.
 */
public class WhiskyService extends AbstractVerticle
{

   private JDBCClient jdbc;
   WhiskyRepository whiskyRepository;

   @Override
   public void start(Future<Void> fut)
   {
      jdbc = JDBCClient.createShared(vertx, config(), "My-Whisky-Collection");
      whiskyRepository = new WhiskyRepository(jdbc);
      whiskyRepository.startBackend(
               (connection) -> whiskyRepository.createSomeData(connection,
                        (nothing) -> startWebApp(
                                 (http) -> completeStartup(http, fut)
                        ), fut
               ), fut);
   }

   private void startWebApp(Handler<AsyncResult<HttpServer>> next)
   {
      // Create a router object.
      Router router = Router.router(vertx);

      router.route("/assets/*")
               .handler(StaticHandler.create("assets"));

      router.get("/api/whiskies")
               .handler(this::getAll);
      router.route("/api/whiskies*")
               .handler(BodyHandler.create());

      router.post("/api/whiskies").handler(this::addOne);
      router.get("/api/whiskies/:id").handler(this::getOne);
      router.put("/api/whiskies/:id").handler(this::updateOne);
      router.delete("/api/whiskies/:id").handler(this::deleteOne);

      // Create the HTTP server and pass the "accept" method to the request handler.
      vertx
               .createHttpServer()
               .requestHandler(router::accept)
               .listen(
                        // Retrieve the port from the configuration,
                        // default to 8080.
                        config().getInteger("http.port", 8080),
                        next::handle
               );
   }

   private void completeStartup(AsyncResult<HttpServer> http,
            Future<Void> fut)
   {
      if (http.succeeded())
      {
         System.out.println("Application started");
         fut.complete();
      }
      else
      {
         fut.fail(http.cause());
      }
   }

   @Override
   public void stop() throws Exception
   {
      jdbc.close();
   }

   private void addOne(RoutingContext routingContext)
   {
      Whisky whisky =
               Json.decodeValue(routingContext.getBodyAsString(),
                        Whisky.class);
      whiskyRepository.insert(whisky, (r) ->
               routingContext.response()
                        .setStatusCode(201)
                        .putHeader("content-type",
                                 "application/json; charset=utf-8")
                        .end(Json.encodePrettily(r.result()))
      );

   }

   private void getOne(RoutingContext routingContext)
   {
      String id = routingContext.request().getParam("id");
      if (id == null)
      {
         routingContext.response().setStatusCode(400).end();
         return;
      }
      whiskyRepository.fetch(id, result -> {
         if (result.succeeded())
         {
            routingContext.response()
                     .setStatusCode(200)
                     .putHeader("content-type",
                              "application/json; charset=utf-8")
                     .end(Json.encodePrettily(result.result()));
         }
         else
         {
            routingContext.response()
                     .setStatusCode(404).end();
         }
      });
   }

   private void updateOne(RoutingContext routingContext)
   {
      String id = routingContext.request().getParam("id");
      JsonObject json = routingContext.getBodyAsJson();
      if (id == null || json == null)
      {
         routingContext.response().setStatusCode(400).end();
         return;
      }
      whiskyRepository.update(id, json,
               (whisky) -> {
                  if (whisky.failed())
                  {
                     routingContext.response().setStatusCode(404)
                              .end();
                  }
                  else
                  {
                     routingContext.response()
                              .putHeader("content-type",
                                       "application/json; charset=utf-8")
                              .end(Json.encodePrettily(whisky.result()));
                  }
               });
   }

   private void deleteOne(RoutingContext routingContext)
   {
      String id = routingContext.request().getParam("id");
      if (id == null)
      {
         routingContext.response().setStatusCode(400).end();
         return;
      }
      whiskyRepository.delete(id,
               result -> routingContext.response()
                        .setStatusCode(204).end());

   }

   private void getAll(RoutingContext routingContext)
   {
      jdbc.getConnection(ar -> {
         SQLConnection connection = ar.result();
         connection.query("SELECT * FROM Whisky", result -> {
            List<Whisky> whiskies =
                     result.result().getRows().stream()
                              .map(Whisky::new).collect(Collectors.toList());
            routingContext.response()
                     .putHeader("content-type",
                              "application/json; charset=utf-8")
                     .end(Json.encodePrettily(whiskies));
         });
      });
   }

}
