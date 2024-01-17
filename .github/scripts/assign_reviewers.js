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

async function run(github, core, fs) {
  try {
    const token = core.getInput('github-token', {required: true});
    const octokit = github.getOctokit(token);

    // Pick two reviewers from list
    const numberOfReviewers = 2;
    const repo = github.context.repo;

    // Read CODEOWNERS
    const codeownersContent = fs.readFileSync('.github/CODEOWNERS', 'utf8');
    const lines = codeownersContent.split('\n');

    // Search COMMITTERS
    const placeholderLine = lines.find(line => line.startsWith('COMMITTERS_PLACEHOLDER'));
    if (!placeholderLine) {
      throw new Error("No COMMITTERS found in CODEOWNERS");
    }

    // Extract committers from placeholder line
    const committers = placeholderLine.match(/@[\w-]+/g).map(u => u.substring(1));
    if (committers.length === 0) {
      throw new Error("No committer found in COMMITTERS_PLACEHOLDER");
    }

    // Pick reviewers
    const selectedReviewers = [];
    while (selectedReviewers.length < numberOfReviewers && committers.length > 0) {
      const randomIndex = Math.floor(Math.random() * committers.length);
      selectedReviewers.push(committers.splice(randomIndex, 1)[0]);
    }

    // Assign reviewers Pull Request
    if (github.context.payload.pull_request) {
      const pullRequestNumber = github.context.payload.pull_request.number;
      await octokit.rest.pulls.requestReviewers({
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

module.exports = ({github, core, fs}) => {
  return run(github, core, fs)
}
