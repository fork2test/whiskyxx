package io.flwertx.whisky.service;

import io.flwertx.whisky.model.Whisky;
import io.flwertx.whisky.repository.WhiskyRepository;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.http.HttpServer;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;

public class WhiskyService extends AbstractVerticle
{

   private JDBCClient jdbc;
   private WhiskyRepository whiskyRepository;
   private Router router;
   Logger logger = LoggerFactory.getLogger(getClass());

   public WhiskyService(Router router, JDBCClient jdbc)
   {
      this.router = router;
      this.jdbc = jdbc;
   }

   @Override
   public void start(Future<Void> fut)
   {
      logger.info("START");
      whiskyRepository = new WhiskyRepository(jdbc);
      whiskyRepository.createSomeData(result -> {
         if (result.succeeded())
         {
            startWebApp((start) -> {
               if (start.succeeded())
               {
                  completeStartup(start, fut);
               }
               else
               {
                  System.out.println("error - startWebApp: " + result.cause().getMessage());
               }
            });
         }
         else
         {
            System.out.println("error - createSomeData: " + result.cause().getMessage());
         }
      });
   }

   private void startWebApp(Handler<AsyncResult<HttpServer>> next)
   {
      router.get("/api/whiskies").handler(this::getList);
      router.post("/api/whiskies").handler(this::create);
      router.get("/api/whiskies/:id").handler(this::fecth);
      router.put("/api/whiskies/:id").handler(this::update);
      router.delete("/api/whiskies/:id").handler(this::delete);
      next.handle(Future.succeededFuture());
   }

   private void completeStartup(AsyncResult<HttpServer> http, Future<Void> fut)
   {
      if (http.succeeded())
      {
         logger.info("Application started");
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
      logger.info("STOP");
   }

   private void create(RoutingContext routingContext)
   {
      Whisky whisky =
               Json.decodeValue(routingContext.getBodyAsString(),
                        Whisky.class);
      whiskyRepository.insert(whisky, single -> {
                  if (single.failed())
                  {
                     end404(routingContext);
                     return;
                  }
                  routingContext.response()
                           .setStatusCode(201)
                           .putHeader("content-type",
                                    "application/json; charset=utf-8")
                           .end(Json.encodePrettily(single.result()));
               }
      );

   }

   private void fecth(RoutingContext routingContext)
   {
      String id = routingContext.request().getParam("id");
      if (id == null)
      {
         end404(routingContext);
         return;
      }
      whiskyRepository.fetch(id, result -> {
         if (result.failed())
         {
            end404(routingContext);
            return;
         }
         routingContext.response()
                  .setStatusCode(200)
                  .putHeader("content-type",
                           "application/json; charset=utf-8")
                  .end(Json.encodePrettily(result.result()));
      });
   }

   private void update(RoutingContext routingContext)
   {
      String id = routingContext.request().getParam("id");
      JsonObject json = routingContext.getBodyAsJson();
      if (id == null || json == null)
      {
         end404(routingContext);
         return;
      }
      whiskyRepository.update(id, json,
               updated -> {
                  if (updated.failed())
                  {
                     end404(routingContext);
                     return;
                  }
                  routingContext.response()
                           .putHeader("content-type",
                                    "application/json; charset=utf-8")
                           .end(Json.encodePrettily(updated.result()));

               });
   }

   private void delete(RoutingContext routingContext)
   {
      String id = routingContext.request().getParam("id");
      if (id == null)
      {
         end404(routingContext);
         return;
      }
      whiskyRepository.delete(id,
               deleted -> {
                  if (deleted.failed())
                  {
                     end404(routingContext);
                     return;
                  }
                  routingContext.response()
                           .setStatusCode(204).end();
               }
      );

   }

   private void getList(RoutingContext routingContext)
   {

      whiskyRepository.getList(list -> {
         if (list.failed())
         {
            end404(routingContext);
            return;
         }
         routingContext.response()
                  .setStatusCode(200)
                  .putHeader("content-type",
                           "application/json; charset=utf-8")
                  .end(Json.encodePrettily(list.result()));
      });
   }

   private void end404(RoutingContext routingContext)
   {
      routingContext.response()
               .setStatusCode(404).end();
   }

}
