const repoUrl = "https://github.com/monix/monix-connect";

const apiUrl = "api/monix/connect/index.html"

const siteConfig = {
  title: "Monix Connect",
  tagline: "A set of connectors and stream integrations for Monix.",
  url: "https://monix.github.io/monix-connect",
  baseUrl: "/monix-connect/",
  cname: "monix.github.io/monix-connect",

  customDocsPath: "monix-connect-docs/target/mdoc",

  projectName: "monix-connect",
  organizationName: "monix",

  headerLinks: [
    { href: apiUrl, label: "API Docs" },
    { doc: "overview", label: "Documentation" },
    { href: repoUrl, label: "GitHub" }
  ],

  headerIcon: "img/monix-logo.svg",
  titleIcon: "img/monix-logo.svg",
  favicon: "img/monix-logo.png",

  colors: {
    primaryColor: "#122932",
    secondaryColor: "#153243"
  },

  copyright: `Copyright © 2020-${new Date().getFullYear()} The Monix Connect Developers.`,

  highlight: { theme: "github" },

  onPageNav: "separate",

  separateCss: ["api"],

  cleanUrl: true,

  repoUrl,

  apiUrl
};

module.exports = siteConfig;
