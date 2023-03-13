/*
 * Copyright 2022 Datafuse Labs
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// @ts-check
// Note: type annotations allow type checking and IDEs autocompletion

const lightCodeTheme = require('prism-react-renderer/themes/github');
const darkCodeTheme = require('prism-react-renderer/themes/dracula');
const repoAddress = 'https://github.com/datafuselabs/opendal';

/** @type {import('@docusaurus/types').Config} */
const config = {
  title: 'Apache OpenDAL',
  tagline: 'Open Data Access Layer: Access data freely, painlessly, and efficiently',
  favicon: 'img/favicon.ico',

  // Set the production url of your site here
  url: 'https://opendal.apache.org',
  // Set the /<baseUrl>/ pathname under which your site is served
  // For GitHub pages deployment, it is often '/<projectName>/'
  baseUrl: '/',

  // GitHub pages deployment config.
  // If you aren't using GitHub pages, you don't need these.
  organizationName: 'Apache', // Usually your GitHub org/username.
  projectName: 'OpenDal', // Usually your repo name.

  onBrokenLinks: 'warn',
  onBrokenMarkdownLinks: 'warn',

  // Even if you don't use internalization, you can use this field to set useful
  // metadata like html lang. For example, if your site is Chinese, you may want
  // to replace "en" with "zh-Hans".
  i18n: {
    defaultLocale: 'en',
    locales: ['en'],
  },

  presets: [
    [
      '@docusaurus/preset-classic',
      /** @type {import('@docusaurus/preset-classic').Options} */
      ({
        docs: {
          sidebarPath: require.resolve('./sidebars.js'),
          // Please change this to your repo.
          // Remove this to remove the "edit this page" links.
          editUrl:
            'https://github.com/facebook/docusaurus/tree/main/packages/create-docusaurus/templates/shared/',
          showLastUpdateAuthor: true,
          showLastUpdateTime: true
        },
        blog: {
          showReadingTime: true,
          // Please change this to your repo.
          // Remove this to remove the "edit this page" links.
          editUrl:
            'https://github.com/facebook/docusaurus/tree/main/packages/create-docusaurus/templates/shared/',
        },
        theme: {
          customCss: require.resolve('./src/css/custom.css'),
        },
        sitemap: {
          changefreq: 'daily',
          priority: 0.5,
          ignorePatterns: ['/tags/**'],
          filename: 'sitemap.xml',
        },
      }),
    ],
  ],

  themeConfig:
    /** @type {import('@docusaurus/preset-classic').ThemeConfig} */
    ({
      // Replace with your project's social card
      // image: 'img/docusaurus-social-card.jpg',
      colorMode: {
        defaultMode: 'light',
        disableSwitch: true
      },
      navbar: {
        title: 'OpenDAL (incubating)',
        // logo: {
        //   alt: 'My Site Logo',
        //   src: 'img/logo.svg',
        // },
        items: [
          {
            docId: 'Overview',
            position: 'right',
            label: 'Doc',
            items: [
              {
                type: 'doc',
                label: 'Project Docs',
                docId: 'Overview'
              },
              {
                label: 'RFCs',
                to: 'https://opendal.databend.rs/opendal/docs/rfcs/index.html'
              },
              // {
              //   type: 'html',
              //   value: '<hr class="dropdown-separator">'
              // },
              {
                label: 'Node.js',
                to: 'https://www.npmjs.com/package/opendal'
              },
              {
                label: 'Python',
                to: 'https://pypi.org/project/opendal/'
              },
              {
                label: 'Rust',
                to: 'https://crates.io/crates/opendal'
              },
            ]
          },
          {
            to: '/blog',
            label: 'Blog',
            position: 'right'
          },
          {
            type: 'dropdown',
            label: 'Community',
            position: 'right',
            items: [
              {
                label: 'Node.js',
                to: 'https://www.npmjs.com/package/opendal'
              },
              {
                label: 'Python',
                to: 'https://pypi.org/project/opendal/'
              },
              {
                label: 'Rust',
                to: 'https://crates.io/crates/opendal'
              },
              {
                type: 'html',
                value: '<hr class="dropdown-separator">'
              },
              {
                type: 'doc',
                docId: 'CONTRIBUTING',
                label: 'How to contribute'
              },
              {
                label: 'Source Code',
                to: repoAddress
              },
              {
                label: 'Issues Tracker',
                to: `${repoAddress}/issues/`
              }
            ]
          },
          {
            to: `${repoAddress}/releases`,
            label: 'Releases',
            position: 'right'
          },
          {
            href: repoAddress,
            position: 'right',
            className: 'header-github-link',
            'aria-label': 'GitHub repository',
          },
        ],
      },
      footer: {
        style: 'light',
        links: [
          {
            title: 'ASF',
            items: [
              {
                label: 'Apache Software Foundation',
                to: 'https://www.apache.org/'
              },
              {
                label: 'License',
                to: 'https://www.apache.org/licenses/'
              },
              {
                label: 'Events',
                to: 'https://www.apache.org/events/current-event'
              },
              {
                label: 'Security',
                to: 'https://www.apache.org/security/'
              },
              {
                label: 'Sponsorship',
                to: 'https://www.apache.org/foundation/sponsorship.html'
              },
              {
                label: 'Thanks',
                to: 'https://www.apache.org/foundation/thanks.html'
              },
            ]
          },
          {
            title: 'Docs',
            items: [
              {
                label: 'Stable',
                to: '/docs/intro',
              },
              {
                label: 'Main',
                to: '/docs/intro',
              },
            ],
          },
          {
            title: 'Community',
            items: [
              {
                label: 'Stack Overflow',
                to: 'https://stackoverflow.com/questions/tagged/opendal',
              },
              {
                label: 'Discord',
                to: 'https://discord.gg/XQy8yGR2dg',
              },
              {
                label: 'Twitter',
                to: 'https://twitter.com/OnlyXuanwo',
              },
            ],
          },
          {
            title: 'More',
            items: [
              {
                label: 'Blog',
                to: '/blog',
              },
              {
                label: 'GitHub',
                to: repoAddress,
              },
            ],
          },
        ],
        logo: {
          alt: 'Apache Software Foundation',
          src: 'https://www.apache.org/foundation/press/kit/asf_logo.png',
          href: 'https://www.apache.org/',
          width: 150,
        },
        copyright: `Copyright © 2022-${new Date().getFullYear()}, The Apache Software Foundation Apache OpenDAL, OpenDAL, Apache, the Apache feather, and the Apache OpenDAL project logo are either registered trademarks or trademarks of the Apache Software Foundation.`,
      },
      prism: {
        theme: lightCodeTheme,
        darkTheme: darkCodeTheme,
        additionalLanguages: ['rust'],
      },
    }),
};

module.exports = config;
