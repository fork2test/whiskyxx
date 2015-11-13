package io.vertx.blog.first;

import io.vertx.blog.first.service.WhiskyService;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.ext.shell.ShellVerticle;

/**
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class MainVerticle extends AbstractVerticle
{

   @Override
   public void start() throws Exception
   {
      vertx.deployVerticle(ShellVerticle.class.getName(),
               new DeploymentOptions().setConfig(config()));
      vertx.deployVerticle(WhiskyService.class.getName(),
               new DeploymentOptions().setConfig(config()));

   }
}
