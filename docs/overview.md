---
id: overview
title: Overview
---

**Monix Connect** is an initiative to implement stream integrations for [Monix](https://monix.io/).

 A _connector_ is described by a set of utilities that allow *Monix* users to easily integrate their applications with specific data sources, 
 which could be a file, a database or everything which implies interacting, sending or receiving information. 
 
 Therefore, the aim of this project is to provide with a set of predefined functionalities to work with the most common connectors,
 these would basically reduce boilerplate code and furthermore, will let them to greatly save time and complexity on implementing projects.

 The latest stable version of `monix-connect` is compatible with _Monix 3.3.x_, _Scala 2.12.x_, _2.13.x_ and _Scala 3_  
 It's always recommend to be in the latest version, as it might contain the latest features and bugfixes as well as documentation 
 that reflects them. 
 
Find all release [versions](https://github.com/monix/monix-connect/releases)).
 
 ```scala   
 libraryDependencies += "io.monix" %% "monix-connect" % "0.9.0"
```

⚠️ **Mind that the project isn't yet stable, so binary compatibility is not guaranteed.** ❗
