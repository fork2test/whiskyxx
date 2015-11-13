package io.flwertx.whisky.repository;

import io.flwertx.whisky.model.Whisky;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.SQLConnection;
import io.vertx.ext.sql.UpdateResult;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by fiorenzo on 13/11/15.
 */
public class WhiskyRepository
{
   private JDBCClient jdbcClient;

   public WhiskyRepository(JDBCClient jdbcClient)
   {
      this.jdbcClient = jdbcClient;
   }

   public void createSomeData(Handler<AsyncResult<Void>> next)
   {
      String sql = "CREATE TABLE IF NOT EXISTS Whisky (id INTEGER IDENTITY, name varchar(100), origin varchar (100))";
      jdbcClient.getConnection(asyncresult -> {
         SQLConnection connection = asyncresult.result();
         if (asyncresult.failed())
         {
            next.handle(Future.failedFuture(asyncresult.cause()));
            return;
         }
         connection.execute(sql,
                  ar -> {
                     if (ar.failed())
                     {
                        connection.close();
                        next.handle(Future.failedFuture(ar.cause()));
                        return;
                     }
                     connection.query("SELECT * FROM Whisky",
                              select -> {
                                 if (select.failed())
                                 {
                                    connection.close();
                                    next.handle(Future.failedFuture(select.cause()));
                                    return;
                                 }
                                 if (select.result().getNumRows() == 0)
                                 {
                                    insert(new Whisky("Bowmore 15 Years Laimrig", "Scotland, Islay"),
                                             (v) -> insert(new Whisky("Talisker 57° North", "Scotland, Island"),
                                                      (r) -> next.handle(Future.<Void>succeededFuture())));
                                 }
                                 else
                                 {
                                    connection.close();
                                    next.handle(Future.<Void>succeededFuture());
                                 }
                              });

                  });
      });
   }

   public void getList(Handler<AsyncResult<List<Whisky>>> next)
   {
      String sql = "SELECT * FROM Whisky";
      jdbcClient.getConnection(asyncresult -> {
         if (asyncresult.failed())
         {
            next.handle(Future.<List<Whisky>>failedFuture(asyncresult.cause()));
            return;
         }
         SQLConnection connection = asyncresult.result();
         connection.query(sql, result -> {
            if (result.failed())
            {
               next.handle(Future.failedFuture(result.cause()));
               return;
            }
            List<Whisky> whiskies =
                     result.result().getRows().stream()
                              .map(Whisky::new).collect(Collectors.toList());
            next.handle(Future.succeededFuture(whiskies));
            connection.close();
         });
      });
   }

   public void insert(Whisky whisky, Handler<AsyncResult<Whisky>> next)
   {
      String sql = "INSERT INTO Whisky (name, origin) VALUES ?, ?";
      jdbcClient.getConnection(asyncresult -> {
         if (asyncresult.failed())
         {
            next.handle(Future.<Whisky>failedFuture(asyncresult.cause()));
            return;
         }
         SQLConnection connection = asyncresult.result();
         connection.updateWithParams(sql,
                  new JsonArray().add(whisky.getName()).add(whisky.getOrigin()),
                  (ar) -> {
                     if (ar.failed())
                     {
                        next.handle(Future.failedFuture(ar.cause()));
                        return;
                     }
                     UpdateResult result = ar.result();
                     // Build a new whisky instance with the generated id.
                     Whisky w = new Whisky(result.getKeys().getInteger(0), whisky.getName(), whisky.getOrigin());
                     next.handle(Future.succeededFuture(w));
                     connection.close();
                  });
      });
   }

   public void fetch(String id, Handler<AsyncResult<Whisky>> next)
   {
      String sql = "SELECT * FROM Whisky WHERE id=?";
      jdbcClient.getConnection(asyncresult -> {
         if (asyncresult.failed())
         {
            next.handle(Future.<Whisky>failedFuture(asyncresult.cause()));
            return;
         }
         SQLConnection connection = asyncresult.result();
         connection.queryWithParams(sql, new JsonArray().add(id), ar -> {
            if (ar.failed())
            {
               next.handle(Future.failedFuture("Whisky not found"));
               return;
            }
            if (ar.result().getNumRows() >= 1)
            {
               next.handle(Future.succeededFuture(new Whisky(ar.result().getRows().get(0))));
            }
            else
            {
               next.handle(Future.failedFuture("Whisky not found"));
            }

         });
      });
   }

   public void update(String id, JsonObject content, Handler<AsyncResult<Whisky>> next)
   {
      String sql = "UPDATE Whisky SET name=?, origin=? WHERE id=?";
      jdbcClient.getConnection(asyncresult -> {
         if (asyncresult.failed())
         {
            next.handle(Future.<Whisky>failedFuture(asyncresult.cause()));
            return;
         }
         SQLConnection connection = asyncresult.result();
         connection.updateWithParams(sql,
                  new JsonArray().add(content.getString("name")).add(content.getString("origin")).add(id),
                  update -> {
                     if (update.failed())
                     {
                        next.handle(Future.failedFuture("Cannot update the whisky"));
                        return;
                     }
                     if (update.result().getUpdated() == 0)
                     {
                        next.handle(Future.failedFuture("Whisky not found"));
                        return;
                     }
                     next.handle(
                              Future.succeededFuture(new Whisky(Integer.valueOf(id),
                                       content.getString("name"), content.getString("origin"))));
                     connection.close();
                  });
      });
   }

   public void delete(String id, Handler<AsyncResult<Whisky>> next)
   {
      String sql = "DELETE FROM Whisky WHERE id=?";
      jdbcClient.getConnection(asyncresult -> {
         if (asyncresult.failed())
         {
            next.handle(Future.<Whisky>failedFuture(asyncresult.cause()));
            return;
         }
         SQLConnection connection = asyncresult.result();
         connection.updateWithParams(sql,
                  new JsonArray().add(id),
                  delete -> {
                     if (delete.failed())
                     {
                        next.handle(Future.failedFuture("Cannot delete the whisky"));
                        return;
                     }
                     next.handle(Future.succeededFuture());
                     connection.close();
                  });
      });
   }

}
