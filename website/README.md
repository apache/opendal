# Apache OpenDAL Website

This website is built using [Docusaurus 2](https://docusaurus.io/), a modern static website generator.

## Installation

```
$ pnpm install
```

## Local Development

```
$ pnpm start
```

This command starts a local development server and opens up a browser window. Most changes are reflected live without having to restart the server.

## Build

```
$ pnpm build
```

This command generates static content into the `build` directory and can be served using any static contents hosting service.

## Images and badges

Website images are resolved locally by `plugins/local-images.js`. The resolver runs before Markdown compilation and on the generated API documentation, so client navigation and standalone API pages use the same resources. It does not download images during a build.

- Project images use files in `static/img/`, with the current website base URL.
- Badges are generated as self-contained SVG data URLs. Version badges show the version of the documented Rust, Node.js, or Ruby package, read from that package's manifest. CI, package-download, and Discord badges are fixed links rather than live metrics.
- Unknown external images fail the build with their page and URL. Add a local asset or an explicit badge presentation to the resolver instead of adding a network fallback.

Run `pnpm test` to check image rewriting, independent package versions, and offline operation. To test a nested deployment path, run `OPENDAL_WEBSITE_BASE_URL=/opendal/opendal-docs-stable/ pnpm build`.

## Dependencies

Generate dependencies by `npx license-checker --production --excludePrivatePackages --csv > DEPENDENCIES.node.csv`


## For content search

Since search plugin can not work with `pnpm start`, for testing please run `pnpm build && pnpm serve`.
