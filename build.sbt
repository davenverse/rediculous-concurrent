val catsV = "2.3.0"
val catsEffectV = "2.3.1"
val shapelessV = "2.3.3"
val fs2V = "2.4.6"
val http4sV = "0.21.15"
val circeV = "0.13.0"
val doobieV = "0.9.4"
val log4catsV = "1.1.1"
val mUnitV = "0.7.20"

ThisBuild / testFrameworks += new TestFramework("munit.Framework")

ThisBuild / crossScalaVersions := Seq("2.13.4")

// Projects
lazy val `rediculous-concurrent` = project.in(file("."))
  .disablePlugins(MimaPlugin)
  .enablePlugins(NoPublishPlugin)
  .aggregate(core, examples)

lazy val core = project.in(file("core"))
  .settings(
    name := "rediculous-concurrent",
    libraryDependencies ++= Seq(
      "org.typelevel"               %% "cats-core"                  % catsV,
      "org.typelevel"               %% "cats-effect"                % catsEffectV,

      "co.fs2"                      %% "fs2-core"                   % fs2V,
      "co.fs2"                      %% "fs2-io"                     % fs2V,

      "io.circe"                    %% "circe-core"                 % circeV,
      "io.circe"                    %% "circe-parser"               % circeV,

      "io.chrisdavenport"           %% "rediculous"                 % "0.0.11",
      "io.chrisdavenport"           %% "mapref"                     % "0.1.1",
      "io.chrisdavenport"           %% "circuit"                    % "0.4.3",
      "io.chrisdavenport"           %% "mules"                      % "0.4.0",

      // Deps we may use in the future, but don't need presently.
      // "io.circe"                    %% "circe-generic"              % circeV,
      // "io.chrisdavenport"           %% "log4cats-core"              % log4catsV,
      // "io.chrisdavenport"           %% "log4cats-slf4j"             % log4catsV,
      // "io.chrisdavenport"           %% "log4cats-testing"           % log4catsV     % Test,
      "org.scalameta"               %% "munit"                      % mUnitV        % Test,
      "org.typelevel"               %% "munit-cats-effect-2"        % "0.12.0"      % Test,
      "com.dimafeng"                %% "testcontainers-scala"       % "0.38.8"      % Test
    )
  )

lazy val examples = project.in(file("examples"))
  .dependsOn(core)
  .settings(
    skip in publish := true,
    name := "rediculous-examples"
  )

lazy val site = project.in(file("site"))
  .disablePlugins(MimaPlugin)
  .enablePlugins(NoPublishPlugin)
  .enablePlugins(DavenverseMicrositePlugin)
  .dependsOn(core)
  .settings{
    Seq(
      micrositeDescription := "Redis Concurrency Structures",
    )
  }