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

const path = require("path");
const fs = require("fs/promises");
const { createHash } = require("crypto");
const cheerio = require("cheerio");

const IMAGE_EXTENSIONS = new Set([".png", ".jpg", ".jpeg", ".gif", ".svg", ".webp"]);
const QUOTED_URL_PATTERN = /(["'])(https?:\/\/[^"']+)\1/g;

function parseUrl(value) {
  try {
    return new URL(value);
  } catch {
    return undefined;
  }
}

function isShieldsBadgeUrl(url) {
  return url.hostname === "img.shields.io";
}

function isGitHubActionsBadgeUrl(url) {
  return (
    url.hostname === "github.com" &&
    url.pathname.includes("/actions/workflows/") &&
    url.pathname.endsWith("/badge.svg")
  );
}

function isImageFileUrl(url) {
  return IMAGE_EXTENSIONS.has(path.extname(url.pathname).toLowerCase());
}

function isDownloadableImageUrl(value) {
  const url = parseUrl(value);
  if (!url || (url.protocol !== "http:" && url.protocol !== "https:")) {
    return false;
  }

  return isImageFileUrl(url) || isShieldsBadgeUrl(url) || isGitHubActionsBadgeUrl(url);
}

function getImageExtension(url) {
  if (isShieldsBadgeUrl(url) || isGitHubActionsBadgeUrl(url)) {
    return ".svg";
  }

  const ext = path.extname(url.pathname).toLowerCase();
  return IMAGE_EXTENSIONS.has(ext) ? ext : ".jpg";
}

function getImageFilename(imageUrl) {
  const hash = createHash("md5").update(imageUrl).digest("hex");
  const parsedUrl = parseUrl(imageUrl);
  const ext = parsedUrl ? getImageExtension(parsedUrl) : ".jpg";
  return `${hash}${ext}`;
}

function existsAsync(pathname) {
  return fs.access(pathname).then(
    () => true,
    () => false
  );
}

async function walkFiles(dir, predicate, files = []) {
  const entries = await fs.readdir(dir, { withFileTypes: true });

  for (const entry of entries) {
    const fullPath = path.join(dir, entry.name);
    if (entry.isDirectory()) {
      await walkFiles(fullPath, predicate, files);
    } else if (predicate(entry.name, fullPath)) {
      files.push(fullPath);
    }
  }

  return files;
}

function collectQuotedImageUrls(content) {
  return Array.from(content.matchAll(QUOTED_URL_PATTERN))
    .map((match) => ({
      original: match[0],
      imageUrl: match[2],
    }))
    .filter(({ imageUrl }) => isDownloadableImageUrl(imageUrl));
}

module.exports = function (_context) {
  const processedImages = new Map();

  async function downloadImage(imageUrl, buildDir) {
    if (processedImages.has(imageUrl)) {
      return processedImages.get(imageUrl);
    }

    try {
      const filename = getImageFilename(imageUrl);
      const buildImagesDir = path.join(buildDir, "img/external");
      const buildOutputPath = path.join(buildImagesDir, filename);

      await fs.mkdir(buildImagesDir, { recursive: true });

      if (!(await existsAsync(buildOutputPath))) {
        console.log(`Downloading image: ${imageUrl}`);

        const controller = new AbortController();
        const timeoutId = setTimeout(() => controller.abort(), 20000);

        try {
          const response = await fetch(imageUrl, {
            signal: controller.signal,
            headers: {
              "User-Agent":
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
              Accept: "image/webp,image/apng,image/*,*/*;q=0.8",
            },
            redirect: "follow",
          });

          clearTimeout(timeoutId);

          if (!response.ok) {
            throw new Error(`HTTP error! Status: ${response.status}`);
          }

          const buffer = await response.arrayBuffer();
          await fs.writeFile(buildOutputPath, Buffer.from(buffer));
        } catch (fetchError) {
          clearTimeout(timeoutId);
          throw fetchError;
        }
      }

      const localUrl = `/img/external/${filename}`;
      processedImages.set(imageUrl, localUrl);
      return localUrl;
    } catch (error) {
      console.error(`Error downloading image ${imageUrl}: ${error.message}`);
      return imageUrl;
    }
  }

  async function processJSFiles(outDir) {
    console.log("Processing JS files for external images...");

    const jsFiles = await walkFiles(outDir, (name) => name.endsWith(".js"));

    for (const jsFile of jsFiles) {
      const content = await fs.readFile(jsFile, "utf8");
      let newContent = content;
      const replacements = [];

      for (const { original, imageUrl } of collectQuotedImageUrls(content)) {
        const localUrl = await downloadImage(imageUrl, outDir);
        if (localUrl !== imageUrl) {
          replacements.push({
            original,
            replacement: original.replace(imageUrl, localUrl),
          });
        }
      }

      replacements.sort((a, b) => b.original.length - a.original.length);

      for (const { original, replacement } of replacements) {
        newContent = newContent.replace(original, replacement);
      }

      if (newContent !== content) {
        await fs.writeFile(jsFile, newContent);
      }
    }
  }

  return {
    name: "docusaurus-ssr-image-plugin",

    async postBuild({ outDir }) {
      console.log("Processing HTML files for external images...");

      const htmlFiles = await walkFiles(outDir, (name) => name.endsWith(".html"));

      for (const htmlFile of htmlFiles) {
        const html = await fs.readFile(htmlFile, "utf8");
        const $ = cheerio.load(html);
        let modified = false;

        const externalImages = $("img").filter((_, el) => {
          const src = $(el).attr("src");
          return src && src.startsWith("http");
        });

        if (externalImages.length === 0) continue;

        const downloadPromises = [];

        externalImages.each((_, img) => {
          const element = $(img);
          const imageUrl = element.attr("src");

          if (!imageUrl || !imageUrl.startsWith("http")) return;

          downloadPromises.push(
            downloadImage(imageUrl, outDir)
              .then((localUrl) => {
                if (localUrl !== imageUrl) {
                  element.attr("src", localUrl);
                  modified = true;
                }
              })
              .catch(() => {})
          );
        });

        await Promise.all(downloadPromises);

        if (modified) {
          await fs.writeFile(htmlFile, $.html());
        }
      }

      await processJSFiles(outDir);

      console.log(`Processed ${processedImages.size} external images`);
    },
  };
};
