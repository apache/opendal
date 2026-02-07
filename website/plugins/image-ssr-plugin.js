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

module.exports = function (_context) {
  const processedImages = new Map();

  function getImageFilename(imageUrl) {
    const hash = createHash("md5").update(imageUrl).digest("hex");
    let ext = ".jpg";

    try {
      const parsedUrl = new URL(imageUrl);
      const pathname = parsedUrl.pathname || "";

      const pathExt = path.extname(pathname);
      if (pathExt) ext = pathExt;

      if (
        imageUrl.includes("img.shields.io") ||
        imageUrl.includes("actions?query") ||
        imageUrl.includes("github/actions/workflow")
      ) {
        ext = ".svg";
      }
    } catch {}

    return `${hash}${ext}`;
  }

  function existsAsync(path) {
    return fs.access(path).then(
      () => true,
      () => false
    );
  }

  async function downloadImage(imageUrl, buildDir) {
    if (processedImages.has(imageUrl)) {
      return processedImages.get(imageUrl);
    }

    try {
      const filename = getImageFilename(imageUrl);
      const buildImagesDir = path.join(buildDir, "img/external");
      const buildOutputPath = path.join(buildImagesDir, filename);

      // Create directory recursively if it doesn't exist
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

    const jsFiles = [];

    async function findJSFiles(dir) {
      const entries = await fs.readdir(dir, { withFileTypes: true });

      for (const entry of entries) {
        const fullPath = path.join(dir, entry.name);
        if (entry.isDirectory()) {
          await findJSFiles(fullPath);
        } else if (entry.name.endsWith(".js")) {
          jsFiles.push(fullPath);
        }
      }
    }

    await findJSFiles(outDir);

    for (const jsFile of jsFiles) {
      const content = await fs.readFile(jsFile, "utf8");
      let modified = false;
      let newContent = content;

      // Look for shield.io and other image URLs with a more comprehensive regex
      const urlPatterns = [
        /"(https?:\/\/[^"]+\.(png|jpg|jpeg|gif|svg|webp))"/g,
        /"(https?:\/\/img\.shields\.io\/[^"]+)"/g,
        /"(https?:\/\/github\.com\/[^"]+\/actions\/workflow[^"]+)"/g,
        /'(https?:\/\/[^']+\.(png|jpg|jpeg|gif|svg|webp))'/g,
        /'(https?:\/\/img\.shields\.io\/[^']+)'/g,
        /'(https?:\/\/github\.com\/[^']+\/actions\/workflow[^']+)'/g,
      ];

      const allReplacements = [];

      for (const pattern of urlPatterns) {
        const matches = Array.from(newContent.matchAll(pattern));

        for (const match of matches) {
          const imageUrl = match[1];
          if (!imageUrl) continue;

          try {
            const localUrl = await downloadImage(imageUrl, outDir);
            if (localUrl !== imageUrl) {
              allReplacements.push({
                original: match[0],
                replacement: match[0].replace(imageUrl, localUrl),
              });
              modified = true;
            }
          } catch (error) {
            console.error(`Error processing URL in JS file: ${error.message}`);
          }
        }
      }

      // Apply replacements from longest to shortest to avoid partial replacements
      allReplacements.sort((a, b) => b.original.length - a.original.length);

      for (const { original, replacement } of allReplacements) {
        newContent = newContent.replace(original, replacement);
      }

      if (modified) {
        await fs.writeFile(jsFile, newContent);
      }
    }
  }

  return {
    name: "docusaurus-ssr-image-plugin",

    async postBuild({ outDir }) {
      console.log("Processing HTML files for external images...");

      const htmlFiles = [];

      async function findHtmlFiles(dir) {
        const entries = await fs.readdir(dir, { withFileTypes: true });

        for (const entry of entries) {
          const fullPath = path.join(dir, entry.name);
          if (entry.isDirectory()) {
            await findHtmlFiles(fullPath);
          } else if (entry.name.endsWith(".html")) {
            htmlFiles.push(fullPath);
          }
        }
      }

      await findHtmlFiles(outDir);

      for (const htmlFile of htmlFiles) {
        const html = await fs.readFile(htmlFile, "utf8");
        let $ = cheerio.load(html);
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

      // Process JS files to update image references in bundled JavaScript
      await processJSFiles(outDir);

      console.log(`Processed ${processedImages.size} external images`);
    },
  };
};
