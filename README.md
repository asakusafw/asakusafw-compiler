# Asakusa Framework Language Project
This project provides a compiler framework for the [Asakusa DSL](https://github.com/asakusafw/asakusafw),
 which includes a pluggable compiler core, runtime libraries, and inspection utilities.

## How to build
```sh
./mvnw clean package
```

## How to import projects into Eclipse
```sh
./mvnw install eclipse:eclipse -DskipTests
```

And then import projects from Eclipse.

## How to build Gradle Plugin

```sh
cd gradle
./gradlew clean build [install] [-PmavenLocal]
```

## Sub projects
* [Asakusa on Spark](https://github.com/asakusafw/asakusafw-spark)
* [Asakusa on M<sup>3</sup>BP](https://github.com/asakusafw/asakusafw-m3bp)

## License
* [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0)

