# Asakusa Vanilla

Asakusa Vanilla is a reference implementation of Asakusa Framework SDK.

This project includes the followings:

* Asakusa Vanilla Runtime
* Asakusa Vanilla Compiler
* Asakusa Vanilla [Gradle](http://gradle.org/) plug-ins

## How to build

* requirements
  * Java SE Development Kit (>= 1.8)

### Maven artifacts


```
cd ..
./mvnw clean install [-DskipTests]
```

### Gradle plug-ins

```sh
cd gradle
./gradlew clean build [install] [-PmavenLocal]
```

## How to use

* requirements
  * Java SE Development Kit >= 1.8

### Gradle build script example

```groovy
group '<your-group>'

buildscript {
    repositories {
        ...
    }
    dependencies {
        classpath 'com.asakusafw.vanilla:asakusa-vanilla-gradle:<project-version>'
    }
}

apply plugin: 'asakusafw-sdk'
apply plugin: 'asakusafw-organizer'
apply plugin: 'asakusafw-vanilla'
...

```
