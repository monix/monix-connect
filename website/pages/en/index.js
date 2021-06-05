const React = require("react");

const CompLibrary = require("../../core/CompLibrary.js");

const variables = require(process.cwd() + "/variables.js");

const MarkdownBlock = CompLibrary.MarkdownBlock;
const Container = CompLibrary.Container;
const GridBlock = CompLibrary.GridBlock;

class HomeSplash extends React.Component {
  render() {
    const { siteConfig, language = "" } = this.props;
    const { baseUrl, docsUrl } = siteConfig;
    const docsPart = `${docsUrl ? `${docsUrl}/` : ""}`;
    const langPart = `${language ? `${language}/` : ""}`;
    const docUrl = doc => `${baseUrl}${docsPart}${langPart}${doc}`;

    const SplashContainer = props => (
      <div className="homeContainer">
        <div className="homeSplashFade">
          <div className="wrapper homeWrapper">{props.children}</div>
        </div>
      </div>
    );

    const ProjectTitle = () => (
      <h2 className="projectTitle">
        <span>
          <img className="projectTitleLogo" src={siteConfig.titleIcon} />
          {siteConfig.title}
        </span>
        <small>{siteConfig.tagline}</small>
      </h2>
    );

    const PromoSection = props => (
      <div className="section promoSection">
        <div className="promoRow">
          <div className="pluginRowBlock">{props.children}</div>
        </div>
      </div>
    );

    const Button = props => (
      <div className="pluginWrapper buttonWrapper">
        <a className="button" href={props.href} target={props.target}>
          {props.children}
        </a>
      </div>
    );

    return (
      <SplashContainer>

        <div className="inner">
          <ProjectTitle siteConfig={siteConfig} />
          <PromoSection>
            <Button href={siteConfig.apiUrl}>API Docs</Button>
            <Button href={docUrl("overview", language)}>Documentation</Button>
            <Button href={siteConfig.repoUrl}>View on GitHub</Button>
          </PromoSection>
        </div>
      </SplashContainer>
    );
  }
}

class Index extends React.Component {
  render() {
    const { config: siteConfig, language = "" } = this.props;
    const { baseUrl } = siteConfig;

    const index = `
                    [![Release](https://img.shields.io/github/v/tag/monix/monix-connect.svg)](https://search.maven.org/search?q=a:monix-connect*)
                    [![Build](https://github.com/monix/monix-connect/workflows/build/badge.svg)](https://github.com/monix/monix-connect/actions?query=branch%3Amaster+workflow%3Abuild)
                    [![Discord](https://img.shields.io/discord/632277896739946517.svg?label=&logo=discord&logoColor=ffffff&color=404244&labelColor=6A7EC2)](https://discord.gg/wsVZSEx4Nw)
                    [![Join the chat](https://badges.gitter.im/monix/monix-connect.svg)](https://gitter.im/monix/monix-connect?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge)
                    [![Scala Steward badge](https://img.shields.io/badge/Scala_Steward-helping-blue.svg?style=flat&logo=data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAA4AAAAQCAMAAAARSr4IAAAAVFBMVEUAAACHjojlOy5NWlrKzcYRKjGFjIbp293YycuLa3pYY2LSqql4f3pCUFTgSjNodYRmcXUsPD/NTTbjRS+2jomhgnzNc223cGvZS0HaSD0XLjbaSjElhIr+AAAAAXRSTlMAQObYZgAAAHlJREFUCNdNyosOwyAIhWHAQS1Vt7a77/3fcxxdmv0xwmckutAR1nkm4ggbyEcg/wWmlGLDAA3oL50xi6fk5ffZ3E2E3QfZDCcCN2YtbEWZt+Drc6u6rlqv7Uk0LdKqqr5rk2UCRXOk0vmQKGfc94nOJyQjouF9H/wCc9gECEYfONoAAAAASUVORK5CYII=)](https://scala-steward.org)
        `.trim();

    const {
      organization,
      coreModuleName,
      latestVersion,
      scalaPublishVersions
    } = variables;

    const latestVersionBadge = latestVersion
      .replace("-", "--")
      .replace("_", "__");

    const Block = props => (
      <Container
        padding={[]}
        id={props.id}
        background={props.background}
      >
        <GridBlock
          align="top"
          contents={props.children}
          layout={props.layout}
        />
      </Container>
    );

    const Layer0 = () => (
      <Block layout="fourColumn" >
        {[
          {
            image: `${baseUrl}img/elasticsearch.png`,
            imageAlign: 'left'
          },
          {
            image: `${baseUrl}img/dynamodb.png`,
            imageAlign: 'right'
          },
          {
            image: `${baseUrl}img/hadoop.png`,
            imageAlign: 'right',
          }
        ]}
      </Block>
    );

    const Layer1 = () => (
      <Block layout="threeColumn">
        {[
            {
                    image: `${baseUrl}img/gcs.png`,
                    imageAlign: 'left'
                  },
                ,
                {
                  image: `${baseUrl}img/s3.png`,
                  imageAlign: 'left'
                },
          {
            image: `${baseUrl}img/parquet.png`,
            imageAlign: 'right'
          }

        ]}
      </Block>
    );


    const Layer2 = () => (
      <Block layout="fourColumn">
        {[
          {
             image: `${baseUrl}img/dynamo.png`,
             imageAlign: 'left',
             imageLink: 'https://connect.monix.io/docs/dynamodb'
           },

          {
            image: `${baseUrl}img/elasticsearch.png`,
            imageAlign: 'right',
            imageLink: 'https://connect.monix.io/docs/elasticsearch'
          },
          {
            image: `${baseUrl}img/sqs.png`,
            imageAlign: 'right'
          },
          {
            image: `${baseUrl}img/mongo.jpeg`,
            imageAlign: 'right',
          }
          ]}
          </Block>
        );




     return (
          <div>

            <HomeSplash siteConfig={siteConfig} language={language} />
            <div className="mainContainer">

              <div className="index">
                <MarkdownBlock>{index}</MarkdownBlock>
              </div>

              <Layer0 />
              <Layer1 />
              <Layer2 />

            </div>
          </div>
          <Layer2 />
          <Layer1 />
          <Layer0 />
        </div>
      </div>
    );
  }
}

module.exports = Index;
