Triage open GitHub issues and PRs for drt.

## Steps

1. **Open PRs**: `gh pr list --state open`
   - Check CI status for each
   - Check for merge conflicts
   - Check author profiles for spam/bot behavior
   - Flag PRs waiting for review

2. **External comments**: Find comments from non-maintainer users
   ```
   gh api repos/drt-hub/drt/issues/comments --paginate
   ```
   - Filter out masukai, bots
   - Summarize any that need a response

3. **Stale issues**: Issues with no activity in 30+ days
   - Suggest close or re-prioritize

4. **Milestone check**: Are milestones up to date?
   - Current milestone issues: open vs closed
   - Issues without a milestone that should have one

5. **Labels check**: Issues without labels

6. **Report** — summarize:
   - PRs needing action (review, CI fix, merge)
   - Comments needing response
   - Issues to close or re-prioritize
   - Suggested next actions