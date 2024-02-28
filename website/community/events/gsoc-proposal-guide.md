---
title: GSoC Project Proposal Guidance
---

This document contains a short guide on how to structure your OpenDAL Google Summer of Code (GSoC) project proposal and how to increase the chance of your project proposal being accepted.

## Choosing a project

You should start by deciding on which project do you want to work on. You can use our [list of project ideas](https://github.com/apache/opendal/issues/4131)
as an inspiration, or you can come up with your own project idea. However, you should keep in mind that each GSoC project needs at least one mentor available. Therefore, if you come up with a completely new project idea, you should also try to find someone from the OpenDAL community who could mentor you on the project.

If you decide to propose your own project idea, you're most likely to be able to find a mentor if you can describe clearly the utility of the project to OpenDAL community.

We encourage you to think of your own interesting project ideas! There are plenty of things that can be done within the OpenDAL project and contributors are generally happy to discuss and help you narrow down your thoughts into a concrete proposal. Don't be shy!

## Interacting with the OpenDAL community

If you want to discuss our suggested project ideas or your own idea, you can do so on the [maillist](mailto:dev@opendal.apache.org) or [Discord](https://discord.gg/XQy8yGR2dg). Make sure to listen to the feedback of the mentors, and try to incorporate it in your project proposal.

When communicating on the OpenDAL mail list or discord (and when interacting with the OpenDAL community in general), please remember to be polite and uphold the [ASF Code of Conduct](https://www.apache.org/foundation/policies/conduct). Keep in mind that most OpenDAL contributors (and GSoC OpenDAL project mentors) are volunteers, and work on OpenDAL in their free time, so please treat them with respect and avoid spamming.

## Creating the project proposal

Ultimately, the project proposal is the main deciding factor on whether your project will be accepted or not, so make sure that you put energy into making it as good as possible.

The proposal should contain (at least) the following things:
1) A descriptive title of the project that you want to work on
2) Information about yourself, including:
  - Description of your programming experience, attained education, university and study programme that you're currently studying, etc. (a short CV would be ideal)
  - Link to a portfolio of projects that you have worked on (e.g. a GitHub profile or a personal website)
  - Your knowledge of OpenDAL, since most projects will probably require at least some OpenDAL knowledge
  - Your existing open-source contribution experience. If you have already contributed to some open-source repositories, make sure to include a link to these contributions in your proposal!
  - Your preferred time zone (for communicating with the mentor(s))
  - Contact information
3) **Information about your proposed project**. This should be as detailed as possible, see more details [below](#project-information-and-timeline).
4) Information about other commitments that might affect your ability to work on the project during the GSoC period. These can include vacations, exams, other jobs or internships etc. It's not necessarily an issue to have other commitments, but it would be great to know about them in advance, if possible.

## Project information and timeline

This is the most important part of your project proposal. You should include an abstract that explains your project in one or two paragraphs, and then a very detailed description that explains what exactly do you want to achieve in the proposed project. The proposal should also clearly state the designated mentor(s) for your project (you should get in touch with them before submitting the proposal).

In addition to describing what do you intend to work on in the project, you should also specify the size of the project, according to the GSoC [documentation](https://google.github.io/gsocguides/student/time-management-for-students):
- Small: ~90 hours
- Medium: ~175 hours
- Large: ~350 hours

You should also create an approximate weekly plan of work and a list of deliverables. Recall that the default project duration is 12 weeks, but it can be [extended](https://google.github.io/gsocguides/student/time-management-for-students) (for medium and large projects) by up to 22 weeks.

- Describe a brief outline of the work that you plan to do, and try to estimate how will the work be split in the individual weeks of the project.
- Define milestones that you intend to achieve in specific weeks (e.g. finish X in week 4, deliver Y in the middle of the project, have a final version prepared one week before the end of the project, etc.).
  - You should focus specifically on the midterm point (week 6), because your mentor(s) will evaluate your progress at this time. You should be slightly more than half done at this moment, and have something reasonable to show.
  - In week 11 (one week before the end of the project), you should consider doing a "code freeze", and spend the last week to polish tests and documentation.

Of course, it is quite difficult to predict this timeline exactly in advance, and it is not a problem to modify it while the project runs, but try to guesstimate it to the best of your ability.

Furthermore, let us know what is your intended time commitment for working on the project. How many hours per day can you work on it? Are there specific days of the week when you can work on it? Is there some period of time from May to August where you know in advance that you won't be able to work on it? Please include this information in the proposal.

There is a [Community bonding](https://google.github.io/gsocguides/student/how-gsoc-works) period before the contributors start working on their projects. It is designed to help you learn about the community that you're going to contribute to, and to start familiarizing yourself with the code and/or technology of your project. Please include a short description of preparatory work that you intend to work on during this community bonding period (should your project be accepted).

## How to increase your chance of being accepted?

You can demonstrate your dedication (and ability) to work on the selected project proposal by contributing something related to it before your proposal is evaluated. This can encompass e.g. sending a pull request to the relevant repository, fixing a bug, writing documentation, etc. There is no specific template for these kinds of contributions, and it might not be possible to do for all types of projects. You can coordinate with the project mentors to find out if they can suggest some entry-level task for you.

You can also tell us more about your motivation in your proposal. Why did you choose OpenDAL for a GSoC project specifically? Do you like the OpenDAL language? Is the specific project that you want to work on sympathetic to you for some reason? We would like to know!

## Don't forget to submit!

You will need to submit your project proposal through the [Google Summer of Code](https://summerofcode.withgoogle.com/) website. Please keep the **deadline** (**2nd April 2024**) in mind, as there will be no extensions!

Good luck! :)

## How to decrease your chance of being accepted?

There are some actions and behaviours that will make it much less likely that your application will be considered, so you should avoid these. For example:

- Spamming or harassing mentors or other members of the OpenDAL community.
- Letting AI automatically generate your project proposal (you should put effort in it, don't be lazy!).
- Suggesting unreasonably grandiose project proposals, e.g. adding a garbage collector to OpenDAL.
- Suggesting unreasonably trivial project proposals, e.g. fixing a typo in the OpenDAL documentation. Remember that even the smallest [project size](https://google.github.io/gsocguides/student/time-management-for-students) should take about 90 hours!

> This guide was inspired by https://github.com/rust-lang/google-summer-of-code/blob/main/proposal-guide.md.
