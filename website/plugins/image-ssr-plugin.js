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

module.exports = function () {
  return {
    name: "docusaurus-image-ssr",

    async postBuild({ outDir }) {
      console.log("Localizing external images in build output...");

      const imagesDir = path.join(outDir, "img/external");
      await fs.ensureDir(imagesDir);

      const imageCache = new Map();

      async function downloadImage(url) {
        if (imageCache.has(url)) {
          return imageCache.get(url);
        }

        try {
          const hash = createHash("md5").update(url).digest("hex");
          const ext = path.extname(url) || ".jpg";
          const filename = `${hash}${ext}`;
          const imagePath = path.join(imagesDir, filename);

          if (!fs.existsSync(imagePath)) {
            console.log(`Downloading: ${url}`);
            const response = await axios({
              url: url,
              responseType: "arraybuffer",
              timeout: 15000,
              headers: {
                "User-Agent":
                  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
              },
            });

            await fs.writeFile(imagePath, response.data);
          }

          const localUrl = `/img/external/${filename}`;
          imageCache.set(url, localUrl);
          return localUrl;
        } catch (error) {
          console.error(`Failed to download ${url}: ${error.message}`);
          return url;
        }
      }

      const htmlFiles = [];

      async function findHtmlFiles(dir) {
        const files = await fs.readdir(dir);

        for (const file of files) {
          const filePath = path.join(dir, file);
          const stat = await fs.stat(filePath);

          if (stat.isDirectory()) {
            await findHtmlFiles(filePath);
          } else if (file.endsWith(".html")) {
            htmlFiles.push(filePath);
          }
        }
      }

      await findHtmlFiles(outDir);
      console.log(`Found ${htmlFiles.length} HTML files`);

      for (const htmlFile of htmlFiles) {
        const html = await fs.readFile(htmlFile, "utf8");
        const $ = cheerio.load(html);

        const externalImages = $('img[src^="http"]');
        if (externalImages.length === 0) continue;

        console.log(
          `Processing ${externalImages.length} images in ${htmlFile}`,
        );

        const promises = [];

        externalImages.each((_, img) => {
          const element = $(img);
          const url = element.attr("src");

          promises.push(
            downloadImage(url).then((localUrl) => {
              element.attr("src", localUrl);
              element.attr("data-original-src", url);
            }),
          );
        });

        await Promise.all(promises);

        await fs.writeFile(htmlFile, $.html());
      }

      console.log("Image localization complete!");
    },
  };
};
