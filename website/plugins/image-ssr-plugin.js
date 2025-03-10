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
const fs = require("fs-extra");
const axios = require("axios");
const { createHash } = require("crypto");
const cheerio = require("cheerio");
const url = require("url");

module.exports = function (context) {
  const processedImages = new Map();

  function getImageFilename(imageUrl) {
    const hash = createHash("md5").update(imageUrl).digest("hex");
    let ext = ".jpg";

    try {
      const parsedUrl = url.parse(imageUrl);
      const pathname = parsedUrl.pathname || "";

      const pathExt = path.extname(pathname);
      if (pathExt) ext = pathExt;

      if (
        imageUrl.includes("img.shields.io") ||
        imageUrl.includes("actions?query")
      ) {
        ext = ".svg";
      }
    } catch (e) {}

    return `${hash}${ext}`;
  }

  async function downloadImage(imageUrl, buildDir) {
    if (processedImages.has(imageUrl)) {
      return processedImages.get(imageUrl);
    }

    try {
      const filename = getImageFilename(imageUrl);
      const buildImagesDir = path.join(buildDir, "img/external");
      const buildOutputPath = path.join(buildImagesDir, filename);

      fs.ensureDirSync(buildImagesDir);

      if (!fs.existsSync(buildOutputPath)) {
        console.log(`Downloading image: ${imageUrl}`);

        const response = await axios({
          url: imageUrl,
          responseType: "arraybuffer",
          timeout: 20000,
          headers: {
            "User-Agent":
              "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            Accept: "image/webp,image/apng,image/*,*/*;q=0.8",
          },
          maxRedirects: 5,
          validateStatus: (status) => status < 400,
        });

        await fs.writeFile(buildOutputPath, response.data);
      }

      const localUrl = `/img/external/${filename}`;
      processedImages.set(imageUrl, localUrl);
      return localUrl;
    } catch (error) {
      console.error(`Error downloading image ${imageUrl}: ${error.message}`);
      return imageUrl;
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
              .catch(() => {}),
          );
        });

        await Promise.all(downloadPromises);

        if (modified) {
          await fs.writeFile(htmlFile, $.html());
        }
      }

      console.log(`Processed ${processedImages.size} external images`);
    },
  };
};
