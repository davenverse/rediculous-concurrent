val catsV = "2.6.1"
val catsEffectV = "3.2.2"
val fs2V = "3.1.0"
val circeV = "0.14.1"

ThisBuild / testFrameworks += new TestFramework("munit.Framework")

ThisBuild / crossScalaVersions := Seq("2.12.14", "2.13.8", "3.1.2")

// Projects
lazy val `rediculous-concurrent` = project.in(file("."))
  .disablePlugins(MimaPlugin)
  .enablePlugins(NoPublishPlugin)
  .aggregate(core.jvm, core.js, examples)

lazy val core = crossProject(JVMPlatform, JSPlatform)
  .crossType(CrossType.Pure)
  .in(file("core"))
  .settings(yPartial)
  .settings(yKindProjector)
  .settings(
    name := "rediculous-concurrent",
    libraryDependencies ++= Seq(
      "org.typelevel"               %%% "cats-core"                  % catsV,
      "org.typelevel"               %%% "cats-effect"                % catsEffectV,

      "co.fs2"                      %%% "fs2-core"                   % fs2V,
      "co.fs2"                      %%% "fs2-io"                     % fs2V,

      "io.circe"                    %%% "circe-core"                 % circeV,
      "io.circe"                    %%% "circe-parser"               % circeV,

      "io.chrisdavenport"           %%% "rediculous"                 % "0.2.0",
      "io.chrisdavenport"           %%% "mapref"                     % "0.2.1",
      "io.chrisdavenport"           %%% "circuit"                    % "0.5.0",
      "io.chrisdavenport"           %%% "mules"                      % "0.5.0",
      "io.chrisdavenport"           %%% "single-fibered"             % "0.1.0",

      // Deps we may use in the future, but don't need presently.
      // "io.circe"                    %% "circe-generic"              % circeV,
      // "io.chrisdavenport"           %% "log4cats-core"              % log4catsV,
      // "io.chrisdavenport"           %% "log4cats-slf4j"             % log4catsV,
      // "io.chrisdavenport"           %% "log4cats-testing"           % log4catsV     % Test,
      "org.typelevel"               %%% "munit-cats-effect-3"        % "1.0.5"      % Test,
      "io.chrisdavenport"           %%% "whale-tail-manager"         % "0.0.8" % Test,
      // "com.dimafeng"                %% "testcontainers-scala"       % "0.38.8"      % Test
    )
  ).jsSettings(
    scalaJSLinkerConfig ~= { _.withModuleKind(ModuleKind.CommonJSModule)}
  ).jvmSettings(
    libraryDependencies += "com.github.jnr" % "jnr-unixsocket" % "0.38.15" % Test,
  )

lazy val examples = project.in(file("examples"))
  .dependsOn(core.jvm)
  .settings(
    publish / skip := true,
    name := "rediculous-examples"
  )

lazy val site = project.in(file("site"))
  .disablePlugins(MimaPlugin)
  .enablePlugins(NoPublishPlugin)
  .enablePlugins(DavenverseMicrositePlugin)
  .dependsOn(core.jvm)
  .settings{
    Seq(
      micrositeDescription := "Redis Concurrency Structures",
    )
  }

lazy val yPartial = 
  Seq(
    scalacOptions ++= {
      if (scalaVersion.value.startsWith("2.12")) Seq("-Ypartial-unification")
      else Seq()
    }
  )

lazy val yKindProjector =
  Seq(
    scalacOptions ++= {
      if(scalaVersion.value.startsWith("3")) Seq("-Ykind-projector")
      else Seq()
    },
    libraryDependencies ++= {
      if(scalaVersion.value.startsWith("3")) Seq()
      else Seq(compilerPlugin("org.typelevel" % "kind-projector" % "0.13.2" cross CrossVersion.full))
    }
  )