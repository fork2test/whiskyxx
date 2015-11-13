package io.flwertx.whisky;

import io.flwertx.whisky.service.WhiskyService;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.shell.ShellVerticle;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.StaticHandler;

/**
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class MainVerticle extends AbstractVerticle
{

   private JDBCClient jdbc;

   @Override
   public void start() throws Exception
   {
      jdbc = JDBCClient.createShared(vertx, config(), "My-Whisky-Collection");

      Router router = Router.router(vertx);

      router.route("/assets/*")
               .handler(StaticHandler.create("assets"));
      router.route("/api*").handler(BodyHandler.create());

      WhiskyService whiskyService = new WhiskyService(router, jdbc);

      vertx.deployVerticle(ShellVerticle.class.getName(),
               new DeploymentOptions().setConfig(config()));
      vertx.deployVerticle(whiskyService,
               new DeploymentOptions().setConfig(config()));

      vertx
               .createHttpServer()
               .requestHandler(router::accept)
               .listen(
                        config().getInteger("http.port", 8080)
               );

   }

   @Override public void stop() throws Exception
   {
      jdbc.close();
   }
}
