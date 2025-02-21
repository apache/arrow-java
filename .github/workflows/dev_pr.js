// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

async function have_comment(github, context, pr_number, tag) {
    console.log(`Looking for existing comment on ${pr_number} with substring ${tag}`);
    const query = `
query($owner: String!, $name: String!, $number: Int!, $cursor: String) {
  repository(owner: $owner, name: $name) {
    pullRequest(number: $number) {
      id
      comments (after:$cursor, first: 50) {
        nodes {
          id
          body
          author {
            login
          }
        }
        pageInfo {
          endCursor
          hasNextPage
        }
      }
    }
  }
}`;
    const tag_regexp = new RegExp(tag);

    let cursor = null;
    let pr_id = null;
    while (true) {
        const result = await github.graphql(query, {
            owner: context.repo.owner,
            name: context.repo.repo,
            number: pr_number,
            cursor,
        });
        pr_id = result.repository.pullRequest.id;
        cursor = result.repository.pullRequest.comments.pageInfo;
        const comments = result.repository.pullRequest.comments.nodes;

        for (const comment of comments) {
            console.log(comment);
            if (comment.author.login === "github-actions" &&
                comment.body.match(tag_regexp) !== null) {
                console.log("Found existing comment");
                return {pr_id, comment_id: comment.id};
            }
        }

        if (!result.repository.pullRequest.comments.hasNextPage ||
            comments.length === 0) {
            break;
        }
    }
    console.log("No existing comment");
    return {pr_id, comment_id: null};
}

async function upsert_comment(github, {pr_id, comment_id}, body, visible) {
    console.log(`Upsert comment (pr_id=${pr_id}, comment_id=${comment_id}, visible=${visible})`);
    if (!visible) {
        if (comment_id === null) return;

        const query = `
mutation makeComment($comment: ID!) {
  minimizeComment(input: {subjectId: $comment, classifier: RESOLVED}) {
    clientMutationId
  }
}`;
        await github.graphql(query, {
            comment: comment_id,
            body,
        });
        return;
    }

    if (comment_id === null) {
        const query = `
mutation makeComment($pr: ID!, $body: String!) {
  addComment(input: {subjectId: $pr, body: $body}) {
    clientMutationId
  }
}`;
        await github.graphql(query, {
            pr: pr_id,
            body,
        });
    } else {
        const query = `
mutation makeComment($comment: ID!, $body: String!) {
  unminimizeComment(input: {subjectId: $comment}) {
    clientMutationId
  }
  updateIssueComment(input: {id: $comment, body: $body}) {
    clientMutationId
  }
}`;
        await github.graphql(query, {
            comment: comment_id,
            body,
        });
    }
}

module.exports = {
    check_title_format: function({core, github, context}) {
        const title = context.payload.pull_request.title;
        if (title.startsWith("MINOR: ")) {
            console.log("PR is a minor PR");
            return {"issue": null};
        }

        const match = title.match(/^GH-([0-9]+): .*$/);
        if (match === null) {
            core.setFailed("Invalid PR title format. Must either be MINOR: or GH-NNN:");
            return {"issue": null};
        }
        return {"issue": parseInt(match[1], 10)};
    },

    apply_labels: async function({core, github, context}) {
        const body = (context.payload.pull_request.body || "").split(/\n/g);
        var has_breaking = false;
        for (const line of body) {
            if (line.trim().startsWith("**This contains breaking changes.**")) {
                has_breaking = true;
                break;
            }
        }
        if (has_breaking) {
            console.log("PR has breaking changes");
            await github.rest.issues.addLabels({
                issue_number: context.payload.pull_request.number,
                owner: context.repo.owner,
                repo: context.repo.repo,
                labels: ["breaking-change"],
            });
        } else {
            console.log("PR has no breaking changes");
        }
    },

    check_labels: async function({core, github, context}) {
        const categories = ["bug-fix", "chore", "dependencies", "documentation", "enhancement"];
        const labels = (context.payload.pull_request.labels || []);
        const required = new Set(categories);
        var found = false;

        for (const label of labels) {
            console.log(`Found label ${label.name}`);
            if (required.has(label.name)) {
                found = true;
                break;
            }
        }

        // Look to see if we left a comment before.
        const comment_tag = "label_helper_comment";
        const maybe_comment_id = await have_comment(github, context, context.payload.pull_request.number, comment_tag);
        console.log("Found comment?");
        console.log(maybe_comment_id);
        const body_text = `
<!-- ${comment_tag} -->
Thank you for opening a pull request!

Please label the PR with one or more of:

${categories.map(c => `- ${c}`).join("\n")}

Also, add the 'breaking-change' label if appropriate.

See [CONTRIBUTING.md](https://github.com/apache/arrow-java/blob/main/CONTRIBUTING.md) for details.
`;

        if (found) {
            console.log("PR has appropriate label(s)");
            await upsert_comment(github, maybe_comment_id, body_text, false);
        } else {
            console.log(body_text);
            await upsert_comment(github, maybe_comment_id, body_text, true);
            core.setFailed("Missing required labels.  See CONTRIBUTING.md");
        }
    },

    check_linked_issue: async function({core, github, context, issue}) {
        console.log(issue);
        if (issue.issue === null) {
            console.log("This is a MINOR PR");
            return;
        }
        const expected = `https://github.com/apache/arrow-java/issues/${issue.issue}`;

        const query = `
query($owner: String!, $name: String!, $number: Int!) {
  repository(owner: $owner, name: $name) {
    pullRequest(number: $number) {
      closingIssuesReferences(first: 50) {
        edges {
          node {
            number
          }
        }
      }
    }
  }
}`;

        const result = await github.graphql(query, {
            owner: context.repo.owner,
            name: context.repo.repo,
            number: context.payload.pull_request.number,
        });
        const issues = result.repository.pullRequest.closingIssuesReferences.edges;
        console.log(issues);

        for (const link of issues) {
            console.log(`PR is linked to ${link.node.number}`);
            if (link.node.number === issue.issue) {
                console.log(`Found link to ${expected}`);
                return;
            }
        }
        console.log(`Did not find link to ${expected}`);
        core.setFailed("Missing link to issue in title");
    },
};
