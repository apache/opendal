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
const fsSync = require("fs");
const { pipeline } = require("stream/promises");
const { createHash } = require("crypto");
const os = require("os");
const readline = require("readline");

module.exports = function (_context) {
  const processedImages = new Map();

  const JS_IMAGE_PATTERN_SOURCE = String.raw`(["'])(https?:\/\/(?:img\.shields\.io\/[^"']+|github\.com\/[^"']+\/actions\/workflow[^"']+|[^"']+\.(?:png|jpg|jpeg|gif|svg|webp)(?:[?#][^"']*)*))\1`;
  const HTML_IMAGE_PATTERN_SOURCE = String.raw`src=(["'])(https?:\/\/(?:img\.shields\.io\/[^"']+|github\.com\/[^"']+\/actions\/workflow[^"']+|[^"']+\.(?:png|jpg|jpeg|gif|svg|webp)(?:[?#][^"']*)*))\1`;

  function createJsImageRegex() {
    return new RegExp(JS_IMAGE_PATTERN_SOURCE, "g");
  }

  function createHtmlImageRegex() {
    return new RegExp(HTML_IMAGE_PATTERN_SOURCE, "gi");
  }

  async function timeIt(label, fn) {
    console.time(label);
    try {
      return await fn();
    } finally {
      console.timeEnd(label);
    }
  }

  let tempFileCounter = 0;

  function createTempPath(filePath) {
    const dir = path.dirname(filePath);
    const unique = [
      Date.now().toString(36),
      process.pid,
      tempFileCounter++,
      Math.random().toString(36).slice(2),
    ].join("-");
    return path.join(dir, `.__image-plugin-${unique}.tmp`);
  }

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
          const attemptLabel = `image-plugin:download:${imageUrl}:attempt-${attempt}`;
          console.time(attemptLabel);
          const abortController = new AbortController();
          const abortTimeout = setTimeout(() => abortController.abort(), 20000);

          try {
            const response = await fetch(imageUrl, {
              signal: abortController.signal,
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
            console.timeEnd(attemptLabel);
            break;
          } catch (fetchError) {
            lastError = fetchError;
            if (console.timeLog) {
              try {
                console.timeLog(attemptLabel);
              } catch {
                // ignore when console.timeLog unsupported
              }
            }
            console.timeEnd(attemptLabel);
            console.warn(
              `Download attempt ${attempt}/${retries} failed for ${imageUrl}: ${fetchError.message}`
            );

            // Clean up potentially corrupted file
            try {
              await fs.unlink(buildOutputPath);
            } catch {
              // Ignore if file doesn't exist
            }
            continue;
          } finally {
            clearTimeout(abortTimeout);
          }
        }

        if (lastError) {
          console.error(
            `All ${retries} attempts failed for ${imageUrl}, preserving remote URL`
          );
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

    await timeIt("image-plugin:scan-files", () => findFiles(dir));

    console.log(
      `Collecting images from ${htmlFiles.length} HTML and ${jsFiles.length} JS files...`
    );
    const allImageUrls = new Set();

    await Promise.all([
      timeIt("image-plugin:collect-html", () =>
        collectImagesFromHtml(htmlFiles, allImageUrls)
      ),
      timeIt("image-plugin:collect-js", () =>
        collectImagesFromJs(jsFiles, allImageUrls)
      ),
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

    await timeIt("image-plugin:download", () =>
      pLimit(concurrency, downloadTasks)
    );

    console.log("Updating files with local image URLs...");
    await Promise.all([
      timeIt("image-plugin:rewrite-html", () => processHtmlFiles(htmlFiles)),
      timeIt("image-plugin:rewrite-js", () => processJSFiles(jsFiles)),
    ]);
  }

  async function collectImagesFromHtml(htmlFiles, imageUrls) {
    if (htmlFiles.length === 0) return;

    const concurrency = Math.min(Math.max(os.cpus().length, 4), 16);
    const tasks = htmlFiles.map((htmlFile) => async () => {
      const content = await fs.readFile(htmlFile, "utf8");
      if (!content.includes('src="http') && !content.includes("src='http")) {
        return;
      }

      const regex = createHtmlImageRegex();
      let match;
      while ((match = regex.exec(content)) !== null) {
        const imageUrl = match[2];
        if (imageUrl) {
          imageUrls.add(imageUrl);
        }
      }
    });

    await pLimit(concurrency, tasks);
  }

  async function collectImagesFromJs(jsFiles, imageUrls) {
    const concurrency = Math.min(Math.max(os.cpus().length, 4), 16);
    const tasks = jsFiles.map((jsFile) => async () => {
      const stream = fsSync.createReadStream(jsFile, { encoding: "utf8" });
      const rl = readline.createInterface({
        input: stream,
        crlfDelay: Infinity,
      });
      const regex = createJsImageRegex();

      try {
        for await (const line of rl) {
          if (!line.includes("http")) {
            continue;
          }

          regex.lastIndex = 0;
          let match;
          while ((match = regex.exec(line)) !== null) {
            const imageUrl = match[2];
            if (imageUrl) {
              imageUrls.add(imageUrl);
            }
          }
        }
      } catch (error) {
        stream.destroy(error);
        throw error;
      } finally {
        if (!stream.destroyed) {
          stream.destroy();
        }
      }
    });

    await pLimit(concurrency, tasks);
  }

  async function processHtmlFiles(htmlFiles) {
    if (htmlFiles.length === 0) return;

    const concurrency = Math.min(Math.max(os.cpus().length, 4), 16);
    const tasks = htmlFiles.map((htmlFile) => async () => {
      const content = await fs.readFile(htmlFile, "utf8");
      if (!content.includes('src="http') && !content.includes("src='http")) {
        return;
      }

      const regex = createHtmlImageRegex();
      const newContent = content.replace(
        regex,
        (fullMatch, quote, imageUrl) => {
          if (!imageUrl) {
            return fullMatch;
          }

          const localUrl = processedImages.get(imageUrl);
          if (!localUrl || localUrl === imageUrl) {
            return fullMatch;
          }

          return `src=${quote}${localUrl}${quote}`;
        }
      );

      if (newContent !== content) {
        await fs.writeFile(htmlFile, newContent);
      }
    });

    await pLimit(concurrency, tasks);
  }

  async function processJSFiles(jsFiles) {
    const concurrency = Math.min(Math.max(os.cpus().length, 2), 8);
    const tasks = jsFiles.map((jsFile) => async () => {
      const tempPath = createTempPath(jsFile);
      const readStream = fsSync.createReadStream(jsFile, { encoding: "utf8" });
      const writeStream = fsSync.createWriteStream(tempPath, {
        encoding: "utf8",
      });
      const closePromise = new Promise((resolve, reject) => {
        writeStream.once("close", resolve);
        writeStream.once("error", reject);
      });
      const rl = readline.createInterface({
        input: readStream,
        crlfDelay: Infinity,
      });
      const regex = createJsImageRegex();
      let modified = false;
      let isFirstLine = true;
      let processingError;

      try {
        for await (const line of rl) {
          let nextLine = line;
          if (line.includes("http")) {
            regex.lastIndex = 0;
            nextLine = line.replace(regex, (fullMatch, quote, imageUrl) => {
              if (!imageUrl) {
                return fullMatch;
              }

              const localUrl = processedImages.get(imageUrl);
              if (!localUrl || localUrl === imageUrl) {
                return fullMatch;
              }

              return `${quote}${localUrl}${quote}`;
            });
          }

          if (nextLine !== line) {
            modified = true;
          }

          if (!isFirstLine) {
            writeStream.write("\n");
          } else {
            isFirstLine = false;
          }

          writeStream.write(nextLine);
        }
      } catch (error) {
        processingError = error;
      } finally {
        if (!writeStream.destroyed) {
          writeStream.end();
        }
        if (!readStream.destroyed) {
          readStream.destroy(processingError);
        }
      }

      try {
        await closePromise;
      } catch (error) {
        if (!processingError) {
          processingError = error;
        }
      }

      if (processingError) {
        await fs.unlink(tempPath).catch(() => {});
        throw processingError;
      }

      if (modified) {
        await fs.rename(tempPath, jsFile);
      } else {
        await fs.unlink(tempPath);
      }
    });

    await pLimit(concurrency, tasks);
  }

  return {
    name: "docusaurus-ssr-image-plugin",

    async postBuild({ outDir }) {
      await timeIt("image-plugin:post-build", () => processFiles(outDir));
      console.log(`Processed ${processedImages.size} external images`);
    },
  };
};
