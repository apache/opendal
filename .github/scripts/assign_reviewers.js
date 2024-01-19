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

// All committers who what to join the review of not owned code.
// More details could be found at https://github.com/apache/opendal/issues/3967.
const candidates = [
  "Xuanwo",
  "Ji-Xinyou",
  "morristai",
  "dqhl76",
  "ClSlaid",
  "Young-Flash",
  "G-XD",
  "oowl",
  "silver-ymz",
];

async function run(github, context, core, fs) {
  try {
    // Pick two reviewers from list
    const numberOfReviewers = 2;
    const repo = context.repo;

    // Pick reviewers
    const selectedReviewers = [];
    while (selectedReviewers.length < numberOfReviewers && candidates.length > 0) {
      const randomIndex = Math.floor(Math.random() * candidates.length);
      selectedReviewers.push(candidates.splice(randomIndex, 1)[0]);
    }

    // Assign reviewers Pull Request
    if (context.payload.pull_request) {
      const pullRequestNumber = context.payload.pull_request.number;
      await github.rest.pulls.requestReviewers({
        owner: repo.owner,
        repo: repo.repo,
        pull_number: pullRequestNumber,
        reviewers: selectedReviewers,
      });
      console.log(`Assigned reviewers: ${selectedReviewers.join(', ')}`);
    }
  } catch (error) {
    core.setFailed(`Action failed with error: ${error}`);
  }
}

module.exports = ({github, context, core, fs}) => {
  return run(github, context, core, fs)
}
