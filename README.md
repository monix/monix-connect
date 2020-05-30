# Monix Connect  



 [![release-badge][]][release] [![workflow-badge][]][workflow] 
 [![Gitter](https://badges.gitter.im/monix/monix-connect.svg)](https://gitter.im/monix/monix-connect?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge)

 [workflow]:                https://github.com/monix/monix-connect/actions?query=branch%3Amaster+workflow%3Abuild
 [workflow-badge]:          https://github.com/monix/monix-connect/workflows/build/badge.svg

   
 [release]:                 https://search.maven.org/search?q=a:monix-connect*
 [release-badge]:           https://img.shields.io/github/v/tag/monix/monix-connect.svg
 
 _Warning:_ Mind that the project is yet in early stages and its API is likely to be changed.
  
[Monix Connect]() is an initiative to implement stream integrations for [Monix](https://monix.io/).

 The `connector` describes a connection between the application and a specific data point, which could be a file, a database or any system in which the appication 
 can interact by sending or receiving information. 
 Therefore, the aim of this project is to implement the most common
 connections that users could need when developing reactive applications with Monix, these would basically reduce boilerplate code and furthermore will let users to greatly save time and complexity in their implementing projects.

 The latest stable version of `monix-connect` is compatible with Monix 3.x, Scala 2.12.x and 2.13.x, you can import 
 all of the connectors by adding the following dependency (find and fill your release [version](https://github.com/monix/monix-connect/releases)):
 
 ```scala   
 libraryDependencies += "io.monix" %% "monix-connect" % "VERSION"
```

But you can also only include to a specific connector to your library dependencies, see below how to do so and how to get started with each of the available [connectors](#Connectors).  

### Connectors
1. [Akka](#Akka)
2. [DynamoDB](#DynamoDB)
3. [Hdfs](#HDFS)
4. [Parquet](#Parquet)
5. [Redis](#Redis)
6. [S3](#S3)

### Contributing

The Monix Connect project welcomes contributions from anybody wishing to
participate.  All code or documentation that is provided must be
licensed with the same license that Monix Connect is licensed with (Apache
2.0, see LICENSE.txt).

People are expected to follow the
[Scala Code of Conduct](./CODE_OF_CONDUCT.md) when
discussing Monix on GitHub, Gitter channel, or other venues.

Feel free to open an issue if you notice a bug, you have a question about the code,
 an idea for an existing connector or even for adding a new one. Pull requests are also
gladly accepted. For more information, check out the
[contributor guide](CONTRIBUTING.md).

## License

All code in this repository is licensed under the Apache License,
Version 2.0. See [LICENCE.txt](./LICENSE.txt).

