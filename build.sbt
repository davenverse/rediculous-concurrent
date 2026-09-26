ThisBuild / tlBaseVersion := "0.5" // your current series x.y

ThisBuild / organization := "io.chrisdavenport"
ThisBuild / organizationName := "Christopher Davenport"
ThisBuild / licenses := Seq(License.MIT)
ThisBuild / developers := List(
  // your GitHub handle and name
  tlGitHubDev("christopherdavenport", "Christopher Davenport")
)
ThisBuild / tlCiReleaseBranches := Seq()

val catsV = "2.13.0"
val catsEffectV = "3.7.1"
val fs2V = "3.14.0"
val circeV = "0.14.16"

ThisBuild / testFrameworks += new TestFramework("munit.Framework")

ThisBuild / crossScalaVersions := Seq("2.13.18", "3.3.8")

// Projects
lazy val `rediculous-concurrent` = tlCrossRootProject
  .aggregate(core, http4s, examples)

lazy val core = crossProject(JVMPlatform, JSPlatform, NativePlatform)
  .in(file("core"))
  .settings(
    name := "rediculous-concurrent",
    libraryDependencies ++= Seq(
      "org.typelevel"               %%% "cats-core"                  % catsV,
      "org.typelevel"               %%% "cats-effect"                % catsEffectV,

      "co.fs2"                      %%% "fs2-core"                   % fs2V,
      "co.fs2"                      %%% "fs2-io"                     % fs2V,

      "io.circe"                    %%% "circe-core"                 % circeV,
      "io.circe"                    %%% "circe-parser"               % circeV,

      "io.chrisdavenport"           %%% "rediculous"                 % "0.6.0",
      "io.chrisdavenport"           %%% "circuit"                    % "0.7.0",
      "io.chrisdavenport"           %%% "mules"                      % "0.8.0",
      "io.chrisdavenport"           %%% "single-fibered"             % "0.3.0",

      // Deps we may use in the future, but don't need presently.
      // "io.circe"                    %% "circe-generic"              % circeV,
      // "io.chrisdavenport"           %% "log4cats-core"              % log4catsV,
      // "io.chrisdavenport"           %% "log4cats-slf4j"             % log4catsV,
      // "io.chrisdavenport"           %% "log4cats-testing"           % log4catsV     % Test,
      "org.typelevel"               %%% "munit-cats-effect"        % "2.2.1"      % Test,
      // "com.dimafeng"                %% "testcontainers-scala"       % "0.38.8"      % Test
    )
  ).jsSettings(
    scalaJSLinkerConfig ~= { _.withModuleKind(ModuleKind.CommonJSModule)}
  ).jvmSettings(
    libraryDependencies += "com.github.jnr" % "jnr-unixsocket" % "0.38.19" % Test,
  ).platformsSettings(JVMPlatform, JSPlatform)(
    libraryDependencies ++= Seq(
      "io.chrisdavenport"           %%% "whale-tail-manager"         % "0.0.14" % Test,
    )
  )

lazy val http4s = crossProject(JVMPlatform, JSPlatform, NativePlatform)
  .crossType(CrossType.Pure)
  .in(file("http4s"))
  .dependsOn(core)
  .settings(
    name := "rediculous-concurrent-http4s",
    libraryDependencies ++= Seq(
      "io.chrisdavenport" %%% "circuit-http4s-client" % "0.7.0",
    )
  )


lazy val examples = crossProject(JVMPlatform, JSPlatform, NativePlatform)
  .crossType(CrossType.Pure)
  .in(file("examples"))
  .disablePlugins(MimaPlugin)
  .enablePlugins(NoPublishPlugin)
  .dependsOn(core, http4s)
  .settings(
    name := "rediculous-examples",
    libraryDependencies ++= Seq(
      "org.http4s" %%% "http4s-ember-client" % "0.23.37",
      "io.chrisdavenport" %%% "crossplatformioapp" % "0.2.0"
    )
  ).jsSettings(
    scalaJSUseMainModuleInitializer := true,
    Compile / mainClass := Some("SingleFiberedExample"),
    scalaJSLinkerConfig ~= { _.withModuleKind(ModuleKind.CommonJSModule)},
  )

lazy val site = project.in(file("site"))
  .enablePlugins(TypelevelSitePlugin)
  .settings(tlSiteIsTypelevelProject := Some(TypelevelProject.Affiliate))
  .dependsOn(core.jvm)
