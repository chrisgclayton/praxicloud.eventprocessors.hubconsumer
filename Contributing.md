Contribution to PraxiCloud Libraries
=====================

You can contribute to .NET Runtime with issues and PRs. Simply filing issues for problems you encounter is a great way to contribute. Contributing implementations is greatly appreciated.

## Contribution "Bar"

Project maintainers will merge changes that improve the product significantly and broadly and that align with the PraxiCloud Libraries Roadmap. 

Maintainers will not merge changes that have narrowly-defined benefits, due to compatibility risk. The PraxiCloud Library codebase is used by several PraxiCloud products (for example, PraxiCloud Metrics and PraxiCloud Distributed). Other developers build products with the libraries too. These chanes may be reverted if they are found to be breaking.

Contributions must also satisfy the other published guidelines defined in this document.

## DOs and DON'Ts

Please do:

* **DO** give priority to the current style of the project or file you're changing even if it diverges from the general guidelines.
* **DO** include tests when adding new features. When fixing bugs, start with
  adding a test that highlights how the current behavior is broken.
* **DO** keep the discussions focused. When a new or related topic comes up
  it's often better to create new issue than to side track the discussion.

Please do not:

* **DON'T** make PRs for style changes.
* **DON'T** commit code that you didn't write. If you find code that you think is a good fit to add to the PraxiCloud Libraries, file an issue and start a discussion before proceeding.
* **DON'T** submit PRs that alter licensing related files or headers. If you believe there's a problem with them, file an issue and we'll be happy to discuss it.

## Breaking Changes

Contributions must maintain API signature and behavioral compatibility. Contributions that include breaking changes will be rejected. Please file an issue to discuss your idea or change if you believe that it may affect managed code compatibility.

## Commit Messages

Please format commit messages to be short, concise and clearly articulate the changes:

```
Summarize change in 50 characters or less

Provide more detail after the first line. Leave one blank line below the
summary and wrap all lines at 72 characters or less.

If the change fixes an issue, leave another blank line after the final
paragraph and indicate which issue is fixed in the specific format
below.

Fix #42
```

Also do your best to factor commits appropriately, not too large with unrelated things in the same commit, and not too small with the same small change applied N times in N different commits.

## Contributor License Agreement

You must sign a [PraxiCloud Contribution License Agreement (CLA)](https://github.com/chrisgclayton/praxicloud) before your PR will be merged. This is a one-time requirement for projects in the PraxiCloud Libraries. You can read more about [Contribution License Agreements (CLA)](http://en.wikipedia.org/wiki/Contributor_License_Agreement) on Wikipedia.

The agreement: [PraxiCloud-contribution-license-agreement.pdf](https://github.com/chrisgclayton/praxicloud/blob/main/PraxiCloud-contribution-license-agreement.pdf)


## File Headers

The following file header is the used for .NET Core. Please use it for new files.

```
// Copyright (c) Christopher Clayton. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.
```

## PR - CI Process

The GitHub Actions (CI) system will automatically perform the required builds and run tests (including the ones you are expected to run) for PRs. Builds and test runs must be clean.

If the CI build fails for any reason, the PR issue will be updated with a link that can be used to determine the cause of the failure.

## PR Feedback

Project maintainers and community members will provide feedback on your change. Community feedback is highly valued. You may see the absence of maintainer feedback if the community has already provided good review feedback.

When discussing changes it is best to be clear and explicit with your feedback. Please be patient with people who might not understand the finer details about your approach to feedback.
