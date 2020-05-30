---
id: overview
title: Overview
---

Monix Connect is an initiative to implement stream integrations for [Monix](https://monix.io/).

 A connector describes the connection between the application and a specific data point, which could be a file, a database or any system in which the appication 
 can interact by sending or receiving information. Therefore, the aim of this project is to catch the most common
 connections that users could need when developing reactive applications with Monix, these would basically reduce boilerplate code and furthermore, will let the users to greatly save time and complexity in their implementing projects.
 
 The latest stable version of `monix-connect` is compatible with Monix 3.x, Scala 2.12.x and 2.13.x, you can import 
 all of the connectors by adding the following dependency (find and fill your release [version](https://github.com/monix/monix-connect/releases)):
 
 ```scala   
 libraryDependencies += "io.monix" %% "monix-connect" % "VERSION"
```

But you would probably only want to add a specific connector to your library dependencies, see how to do so and how to get started with each of the available connectors in the next sections.  
