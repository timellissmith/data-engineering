#!/bin/bash

# This script creates a new branch based on the the type of code that the user is creating.

# It will prompt for the correct values rather than take the parameters.

# It implements the following logic:
# - If it is a hotfix, check out test and create a branch with hotfix/<dev-initals>-<ticket_number>-<short-description>
# - If it is a feature or fix, check out dev and create a branch with <feature/fox>/<dev-initals>-<ticket_number>-<short-description>

# Push the branch to remote.

DRY_RUN=False

if [[ ${DRY_RUN} == "True" ]]; then
  dry_run_string="echo "
else
  dry_run_string=""
fi

function green() {
   printf "\e[32m%s\e[0m\n" "$@"
}

function yellow() {
   printf "\e[33m%s\e[0m\n" "$@"
}

function red() {
   printf "\e[31m%s\e[0m\n" "$@"
}

function bold() {
   printf "\e[1m%s\e[0m\n" "$@"
}

function error_and_die {
  # Exit on failure
  red "ERROR: ${1}"
  exit 1
}

bold "Creating a new code branch:"
bold "Please enter branch type: hotfix, fix or feature:"

read -r branch_type

if [[ $(echo "hotfix fix feature" | grep -w -q "${branch_type}"; echo ${?}) -eq 0 ]]; then
    green "${branch_type} selected"
else
  error_and_die "Please enter a valid branch type: hotfix, fix or feature"
fi

if [[ "${branch_type}" == "hotfix" ]]; then
  readonly source_branch="origin/test"
else
  readonly source_branch="origin/dev"
fi

yellow "Enter your initals for branch name:"
read -r dev_initals
stripped_dev_initals=$(echo "${dev_initals}" | tr " " "_")

yellow "Enter your JIRA Ticket number for branch name (e.g. DEIP122):"
read -r jira_no
stripped_jira_no=$(echo "${jira_no}" | tr " " "_")

yellow "Enter short description of change for branch name:"
read -r branch_desc
stripped_branch_desc=$(echo "${branch_desc}" | tr " " "_")

readonly full_branch_name="${branch_type}/${stripped_dev_initals}-${stripped_jira_no}-${stripped_branch_desc}"
yellow "Creating branch ${full_branch_name}"
${dry_run_string} git checkout -b "${full_branch_name}" "${source_branch}"

yellow "Push branch to remote"
${dry_run_string} git push --set-upstream origin "${full_branch_name}"

green "Branch ${full_branch_name} created off ${source_branch} and pushed to origin"
