---
id: overview
title: Overview
---

_Monix Connect_ is an initiative to implement stream integrations for [Monix](https://monix.io/).

 A _connector_ describes the connection between the application and a specific data point, which could be a file, a database or any system in which the appication 
 can interact by sending or receiving information. 
 
 Therefore, the aim of this project is to provide to the user with the most common
 connections they could need when developing reactive applications with Monix, these would basically reduce boilerplate code and furthermore, will let them to greatly save time and complexity on implementing projects.
 
 The latest stable version of `monix-connect` is compatible with _Monix 3.x_, _Scala 2.12.x_ and _2.13.x_, you can import 
 all of the connectors by adding the following dependency (find and fill your release [version](https://github.com/monix/monix-connect/releases)):
 
 ```scala   
 libraryDependencies += "io.monix" %% "monix-connect" % "0.6.0-RC1"
```

But you would probably only want to add a specific connector to your library dependencies, see how to do so and how to get started with them in the next sections.  

⚠️ **Mind that the project isn't yet stable, so binary compatibility is not guaranteed.** ❗