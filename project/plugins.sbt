resolvers += "Typesafe repository" at "https://repo.typesafe.com/typesafe/releases/"

resolvers += "sbt-plugins" at "http://scalasbt.artifactoryonline.com/scalasbt/sbt-plugin-releases"

resolvers += "sbt-idea" at "http://mpeltonen.github.com/maven/"

addSbtPlugin("com.github.mpeltonen" % "sbt-idea" % "1.6.0")

addSbtPlugin("com.typesafe.sbteclipse" % "sbteclipse-plugin" % "2.3.0")

addSbtPlugin("com.typesafe.sbt" %% "sbt-native-packager" % "1.0.0-M5")

addSbtPlugin("com.github.gseitz" %% "sbt-release" % "0.8.5")

                                                               