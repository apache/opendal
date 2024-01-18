---
title: Nominate PPMC Member
sidebar_position: 3
---

This document mainly introduces how the PPMC member nominate a new PPMC member.

## Start vote about the candidate

Start a vote about the candidate via sending email to: <private@opendal.apache.org>:

- candidate_name: The full name of the candidate.
- candidate_github_id: The github id of the candidate.

Title:

```
[VOTE] Add candidate ${candidate_name} as a new PPMC member
```

Content:

```
Hi, All OpenDAL PPMC members.
  
I would like to nominate ${candidate_name} (github id: ${candidate_github_id}) as a candidate for the OpenDAL PPMC member. Since becoming an OpenDAL committer, Xiangdong has made significant contributions to various modules of the project.

${candidate_contributions}

${candidate_name}'s great contributions could be found:

- Github Account: https://github.com/${candidate_github_id}
- Github Pull Requests: https://github.com/apache/opendal/pulls?q=is%3Apr+author%3A${candidate_github_id}+is%3Aclosed
- Github Issues: https://github.com/apache/opendal/issues?q=is%3Aopen+mentions%3A${candidate_github_id}

Please make your valuable evaluation on whether we could invite ${candidate_name} as a
committer:

[ +1 ] Agree to add ${candidate_name} as a PPMC member of OpenDAL.
[ 0  ] Have no sense.
[ -1 ] Disagree to add ${candidate_name} as a PPMC member of OpenDAL, because .....

This vote starts from the moment of sending and will be open for 3 days.
 
Thanks and best regards,

${your_name}
```

Example: <https://lists.apache.org/thread/yg2gz2tof3cvbrgp1wxzk6mf9o858h7t> (Private Link)

After at least 3 `+1` binding vote and no veto, claim the vote result:

Title:

```
[RESULT][VOTE] Add candidate ${candidate_name} as a new PPMC member
```

Content:

```
Hi, all:

The vote for "Add candidate ${candidate_name} as a new PPMC member" has PASSED and closed now.

The result is as follows:

4 binding +1 Votes:
- voter names

Vote thread: https://lists.apache.org/thread/yg2gz2tof3cvbrgp1wxzk6mf9o858h7t

Then I'm going to invite ${candidate_name} to join us.

Thanks for everyone's support!

${your_name}
```

## Send NOTICE to IPMC after VOTE PASSED

The nominating PPMC member should send a message to the IPMC <private@incubator.apache.org> with a reference to the vote result in the following form:

Title:

```
[NOTICE] ${candidate_name} for OpenDAL PPMC
```

Content:

```
${candidate_name} has been voted as a new member of the OpenDAL PPMC. the vote thread is at: 

https://lists.apache.org/thread/yg2gz2tof3cvbrgp1wxzk6mf9o858h7t
```

## Send invitation to the candidate

Send an invitation to the candidate and cc <private@opendal.apache.org>:

Title:

```
Invitation to become OpenDAL PPMC Member: ${candidate_name}
```

Content:

```
Hello ${candidate_name},

In recognition of your contributions to Apache OpenDAL(incubating), the OpenDAL PPMC has recently voted to add you as a PPMC member. The role of a PPMC member grants you access to the Podling Project Management Committee (PPMC) and enables you to take on greater responsibilities within the OpenDAL project. We hope that you accept this invitation and continue to help us make Apache OpenDAL(incubating) better.

Please reply to private@opendal.apache.org using the 'reply all' function for your responses.

With the expectation of your acceptance, welcome!

${your_name} (as represents of The Apache OpenDAL(incubating) PPMC)
```

## Add the candidate to the PPMC member list

After the candidate accepts the invitation, add the candidate to the PPMC member list by [whimsy roster tools](https://whimsy.apache.org/roster/ppmc/opendal)

![](roster-add-ppmc-member.png)
