# <img src="https://uploads-ssl.webflow.com/5ea5d3315186cf5ec60c3ee4/5edf1c94ce4c859f2b188094_logo.svg" alt="Pip.Services Logo" width="200"> <br/> Redis components for Pip.Services in Java

This module is a part of the [Pip.Services](http://pipservices.org) polyglot microservices toolkit.

The module for working with a Redis database of type key-value contains components for working with cache and locks: RedisCache and RedisLock.

The module contains the following packages:

- **Build** - factory default
- **Cache** - Redis Cache Components
- **Lock** - components of working with locks in Redis

<a name="links"></a> Quick links:

* [Configuration](http://docs.pipservices.org/toolkit/getting_started/configurations/)
* [API Reference](https://pip-services3-java.github.io/pip-services3-redis-java/)
* [Change Log](CHANGELOG.md)
* [Get Help](http://docs.pipservices.org/get_help/)
* [Contribute](http://docs.pipservices.org/contribute/)

## Use

Go to the pom.xml file in Maven project and add dependencies::
```xml
<dependency>
  <groupId>org.pipservices3</groupId>
  <artifactId>pip-services3-redis</artifactId>
  <version>3.0.0</version>
</dependency>
```

## Develop

For development you shall install the following prerequisites:
* Java SE Development Kit 18+
* Eclipse Java Photon or another IDE of your choice
* Docker
* Apache Maven

Build the project:
```bash
mvn install
```

Run automated tests:
```bash
mvn test
```

Generate API documentation:
```bash
./docgen.ps1
```

Before committing changes run dockerized build and test as:
```bash
./build.ps1
./test.ps1
./clear.ps1
```

## Contacts

The initial implementation is done by 
**Sergey Seroukhov**
**Danil Prisiazhnyi**
