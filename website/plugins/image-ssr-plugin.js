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
const { pipeline } = require("stream/promises");
const { createHash } = require("crypto");
const cheerio = require("cheerio");
const os = require("os");

module.exports = function (_context) {
  const processedImages = new Map();

  const IMAGE_URL_PATTERNS = [
    /"(https?:\/\/[^"]+\.(png|jpg|jpeg|gif|svg|webp))"/g,
    /"(https?:\/\/img\.shields\.io\/[^"]+)"/g,
    /"(https?:\/\/github\.com\/[^"]+\/actions\/workflow[^"]+)"/g,
    /'(https?:\/\/[^']+\.(png|jpg|jpeg|gif|svg|webp))'/g,
    /'(https?:\/\/img\.shields\.io\/[^']+)'/g,
    /'(https?:\/\/github\.com\/[^']+\/actions\/workflow[^']+)'/g,
  ];

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

  async function downloadImage(imageUrl, buildDir, retries = 3) {
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

        let lastError;
        for (let attempt = 1; attempt <= retries; attempt++) {
          try {
            const response = await fetch(imageUrl, {
              signal: AbortSignal.timeout(30000),
              headers: {
                "User-Agent":
                  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                Accept: "image/webp,image/apng,image/*,*/*;q=0.8",
              },
              redirect: "follow",
            });

            if (!response.ok) {
              throw new Error(`HTTP error! Status: ${response.status}`);
            }

            const fd = await fs.open(buildOutputPath, "w");
            await pipeline(response.body, fd.createWriteStream());
            lastError = null;
            break;
          } catch (fetchError) {
            lastError = fetchError;

            // Clean up potentially corrupted file
            try {
              await fs.unlink(buildOutputPath);
            } catch {
              // Ignore if file doesn't exist
            }
          }
        }

        if (lastError) {
          throw lastError;
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

  async function pLimit(concurrency, tasks) {
    const results = [];
    const executing = [];

    for (const task of tasks) {
      const p = Promise.resolve().then(() => task());
      results.push(p);

      if (concurrency <= tasks.length) {
        const e = p.then(() => executing.splice(executing.indexOf(e), 1));
        executing.push(e);
        if (executing.length >= concurrency) {
          await Promise.race(executing);
        }
      }
    }

    return Promise.all(results);
  }

  async function processFiles(dir) {
    const htmlFiles = [];
    const jsFiles = [];

    async function findFiles(dir) {
      const entries = await fs.readdir(dir, { withFileTypes: true });

      for (const entry of entries) {
        const fullPath = path.join(dir, entry.name);
        if (entry.isDirectory()) {
          await findFiles(fullPath);
        } else if (entry.name.endsWith(".js")) {
          jsFiles.push(fullPath);
        } else if (entry.name.endsWith(".html")) {
          htmlFiles.push(fullPath);
        }
      }
    }

    await findFiles(dir);

    console.log(
      `Collecting images from ${htmlFiles.length} HTML and ${jsFiles.length} JS files...`
    );
    const allImageUrls = new Set();

    await Promise.all([
      collectImagesFromHtml(htmlFiles, allImageUrls),
      collectImagesFromJs(jsFiles, allImageUrls),
    ]);

    console.log(`Downloading ${allImageUrls.size} unique images...`);
    const downloadTasks = Array.from(allImageUrls).map(
      (url) => () => downloadImage(url, dir)
    );

    const cpuCount = os.cpus().length;
    const concurrency = Math.min(Math.max(cpuCount * 5, 10), 30);
    console.log(
      `Using ${concurrency} concurrent downloads (CPU cores: ${cpuCount})`
    );

    await pLimit(concurrency, downloadTasks);

    console.log("Updating files with local image URLs...");
    await Promise.all([processHtmlFiles(htmlFiles), processJSFiles(jsFiles)]);
  }

  async function traverseHtmlFiles(htmlFiles, handler) {
    for (const htmlFile of htmlFiles) {
      const html = await fs.readFile(htmlFile, "utf8");
      const $ = cheerio.load(html);

      const result = handler(htmlFile, $);

      if (result?.shouldWrite) {
        await fs.writeFile(htmlFile, $.html());
      }
    }
  }

  async function traverseJsFiles(jsFiles, handler) {
    for (const jsFile of jsFiles) {
      const content = await fs.readFile(jsFile, "utf8");

      const result = handler(content);

      if (result?.newContent && result.newContent !== content) {
        await fs.writeFile(jsFile, result.newContent);
      }
    }
  }

  async function collectImagesFromHtml(htmlFiles, imageUrls) {
    await traverseHtmlFiles(htmlFiles, (_, $) => {
      $("img").each((_, el) => {
        const src = $(el).attr("src");
        if (src && src.startsWith("http")) {
          imageUrls.add(src);
        }
      });
    });
  }

  async function collectImagesFromJs(jsFiles, imageUrls) {
    await traverseJsFiles(jsFiles, (content) => {
      for (const pattern of IMAGE_URL_PATTERNS) {
        const matches = Array.from(content.matchAll(pattern));
        for (const match of matches) {
          if (match[1]) imageUrls.add(match[1]);
        }
      }
    });
  }

  async function processHtmlFiles(htmlFiles) {
    await traverseHtmlFiles(htmlFiles, (_, $) => {
      let modified = false;

      $("img").each((_, img) => {
        const element = $(img);
        const imageUrl = element.attr("src");

        if (imageUrl && imageUrl.startsWith("http")) {
          const localUrl = processedImages.get(imageUrl);
          if (localUrl && localUrl !== imageUrl) {
            element.attr("src", localUrl);
            modified = true;
          }
        }
      });

      return { shouldWrite: modified };
    });
  }

  async function processJSFiles(jsFiles) {
    await traverseJsFiles(jsFiles, (content) => {
      let newContent = content;
      const allReplacements = [];

      for (const pattern of IMAGE_URL_PATTERNS) {
        const matches = Array.from(newContent.matchAll(pattern));

        for (const match of matches) {
          const imageUrl = match[1];
          if (!imageUrl) continue;

          const localUrl = processedImages.get(imageUrl);
          if (localUrl && localUrl !== imageUrl) {
            allReplacements.push({
              original: match[0],
              replacement: match[0].replace(imageUrl, localUrl),
            });
          }
        }
      }

      allReplacements.sort((a, b) => b.original.length - a.original.length);

      for (const { original, replacement } of allReplacements) {
        newContent = newContent.replace(original, replacement);
      }

      return { newContent };
    });
  }

  return {
    name: "docusaurus-ssr-image-plugin",

    async postBuild({ outDir }) {
      await processFiles(outDir);

      console.log(`Processed ${processedImages.size} external images`);
    },
  };
};
