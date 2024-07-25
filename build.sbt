ThisBuild / tlBaseVersion := "0.4" // your current series x.y

ThisBuild / organization := "io.chrisdavenport"
ThisBuild / organizationName := "Christopher Davenport"
ThisBuild / licenses := Seq(License.MIT)
ThisBuild / developers := List(
  // your GitHub handle and name
  tlGitHubDev("christopherdavenport", "Christopher Davenport")
)
ThisBuild / tlCiReleaseBranches := Seq("main")

// true by default, set to false to publish to s01.oss.sonatype.org
ThisBuild / tlSonatypeUseLegacyHost := true


val catsV = "2.11.0"
val catsEffectV = "3.4.11"
val fs2V = "3.10.2"
val circeV = "0.14.8"

ThisBuild / testFrameworks += new TestFramework("munit.Framework")

ThisBuild / crossScalaVersions := Seq("2.12.19", "2.13.14", "3.3.3")

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

      "io.chrisdavenport"           %%% "rediculous"                 % "0.5.0",
      "io.chrisdavenport"           %%% "circuit"                    % "0.5.1",
      "io.chrisdavenport"           %%% "mules"                      % "0.7.0",
      "io.chrisdavenport"           %%% "single-fibered"             % "0.1.1",

      // Deps we may use in the future, but don't need presently.
      // "io.circe"                    %% "circe-generic"              % circeV,
      // "io.chrisdavenport"           %% "log4cats-core"              % log4catsV,
      // "io.chrisdavenport"           %% "log4cats-slf4j"             % log4catsV,
      // "io.chrisdavenport"           %% "log4cats-testing"           % log4catsV     % Test,
      "org.typelevel"               %%% "munit-cats-effect"        % "2.0.0-M3"      % Test,
      // "com.dimafeng"                %% "testcontainers-scala"       % "0.38.8"      % Test
    )
  ).jsSettings(
    scalaJSLinkerConfig ~= { _.withModuleKind(ModuleKind.CommonJSModule)}
  ).jvmSettings(
    libraryDependencies += "com.github.jnr" % "jnr-unixsocket" % "0.38.19" % Test,
  ).platformsSettings(JVMPlatform, JSPlatform)(
    libraryDependencies ++= Seq(
      "io.chrisdavenport"           %%% "whale-tail-manager"         % "0.0.9" % Test,
    )
  )

lazy val http4s = crossProject(JVMPlatform, JSPlatform, NativePlatform)
  .crossType(CrossType.Pure)
  .in(file("http4s"))
  .dependsOn(core)
  .settings(
    name := "rediculous-concurrent-http4s",
    libraryDependencies ++= Seq(
      "io.chrisdavenport" %%% "circuit-http4s-client" % "0.5.1",
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
      "org.http4s" %%% "http4s-ember-client" % "0.23.18",
      "io.chrisdavenport" %%% "crossplatformioapp" % "0.1.0"
    )
  ).jsSettings(
    scalaJSUseMainModuleInitializer := true,
    Compile / mainClass := Some("SingleFiberedExample"),
    scalaJSLinkerConfig ~= { _.withModuleKind(ModuleKind.CommonJSModule)},
  )

lazy val site = project.in(file("site"))
  .enablePlugins(TypelevelSitePlugin)
  .dependsOn(core.jvm)


