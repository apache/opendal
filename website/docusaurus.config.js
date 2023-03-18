/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

// @ts-check
// Note: type annotations allow type checking and IDEs autocompletion

const lightCodeTheme = require('prism-react-renderer/themes/github');
const darkCodeTheme = require('prism-react-renderer/themes/dracula');
const repoAddress = 'https://github.com/apache/incubator-opendal';

/** @type {import('@docusaurus/types').Config} */
const config = {
  title: 'Apache OpenDAL',
  tagline: 'Open Data Access Layer: Access data freely, painlessly, and efficiently',
  favicon: 'img/favicon.ico',

  url: 'https://opendal.apache.org/',
  baseUrl: '/',

  organizationName: 'Apache',
  projectName: 'OpenDAL',

  onBrokenLinks: 'warn',
  onBrokenMarkdownLinks: 'warn',

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
          editUrl:
            'https://github.com/apache/incubator-opendal/website/',
          showLastUpdateAuthor: true,
          showLastUpdateTime: true
        },
        blog: {
          showReadingTime: true,
          editUrl:
            'https://github.com/apache/incubator-opendal/website/',
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
      // TODO social card image
      // image: 'img/opendal-social-card.jpg',
      colorMode: {
        defaultMode: 'light',
        disableSwitch: true
      },
      navbar: {
        title: 'OpenDAL (incubating)',
        items: [
          {
            position: 'right',
            label: 'Documentation',
            items: [
              {
                type: 'html',
                value: '<a class="dropdown__link" href="/docs/rust/opendal">Rust</a>'
              },
              {
                type: 'html',
                value: '<a class="dropdown__link" href="/docs/nodejs/">Node.js</a>'
              },
              {
                type: 'html',
                value: '<a class="dropdown__link" href="/docs/python/">Python</a>'
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
                label: 'Source Code',
                to: repoAddress
              },
              {
                label: 'Issues Tracker',
                to: `${repoAddress}/issues/`
              },
              {
                label: 'Code of Conduct',
                to: 'https://www.apache.org/foundation/policies/conduct.html'
              }
            ]
          },
          {
            to: `${repoAddress}/releases`,
            label: 'Releases',
            position: 'right'
          },
          {
            type: 'dropdown',
            label: 'ASF Links',
            position: 'right',
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
            href: repoAddress,
            position: 'right',
            className: 'header-github-link',
            'aria-label': 'GitHub repository',
          },
          {
            href: 'https://discord.gg/XQy8yGR2dg',
            position: 'right',
            className: 'header-discord-link',
            'aria-label': 'Discord',
          },
        ],
      },
      footer: {
        style: 'light',
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
