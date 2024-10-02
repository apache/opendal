# Apache OpenDAL™ Spring Integrations

Apache OpenDAL™ Spring Integrations provide seamless integration between the Apache OpenDAL library and Spring applications. This project offers both synchronous and asynchronous configurations tailored to different Spring environments.

## Overview

This project includes three primary modules:

- opendal-spring: Core integration module for [Spring](https://spring.io/) applications.
- opendal-spring-boot-starter: Synchronous starter for [Spring WebMVC](https://docs.spring.io/spring-framework/reference/web/webmvc.html).
- opendal-spring-boot-starter-reactive: Asynchronous starter for [Spring WebFlux](https://docs.spring.io/spring-framework/reference/web/webflux.html).

## Features

- SpringBoot autoconfiguration support for an OpenDALTemplate/ReactiveOpenDALTemplate instance.

## Prerequisites

This project requires JDK 17 or later and supports Spring 6 and Spring Boot 3.

## Getting Started With Spring Boot Starter

Below is a brief example demonstrating how to use the OpenGemini Spring Boot Starter in a Java application.

### Maven Configuration

Add the following dependency to your project's `pom.xml`:

```xml
<dependency>
    <groupId>org.apache.opendal</groupId>
    <artifactId>opendal-spring-boot-starter</artifactId>
    <version>${version}</version>
</dependency>
```

### SpringBoot Application Configuration

Following properties can be used in your `application.yaml`:

```yaml
spring:
  opendal:
    schema: "fs"
    conf:
      root: "/tmp"
```

## Getting Started With Spring Boot Reactive Starter

Below is a brief example demonstrating how to use the OpenGemini Spring Boot Starter in a Java application.

### Maven Configuration

Add the following dependency to your project's `pom.xml`:

```xml
<dependency>
    <groupId>org.apache.opendal</groupId>
    <artifactId>opendal-spring-boot-starter-reactive</artifactId>
    <version>${version}</version>
</dependency>
```

### SpringBoot Reactive Application Configuration

Following properties can be used in your `application.yaml`:

```yaml
spring:
  opendal:
    schema: "fs"
    conf:
      root: "/tmp"
```
