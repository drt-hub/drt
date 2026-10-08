# Releasing the VS Code extension (maintainers)

The extension versions independently of `drt-core` (see `CHANGELOG.md`). It is
published to the **VS Code Marketplace** (publisher `drt-hub`) and **Open VSX**.
Publishing is currently **manual**; there is no release workflow.

## Why manual

The Marketplace's automated path (`vsce publish`) needs an Azure DevOps
Personal Access Token, and creating an Azure DevOps organization now requires
linking an Azure subscription (a payment method). The project does not carry
one, so the Marketplace is published by uploading the `.vsix` in the web UI,
which needs only the `drt-hub` publisher and its Microsoft account.

## When to release

Release whenever the bundled schemas change in a way users would notice (a new
destination, option or enum value). CI (`vscode-schema-drift`) already forces
the committed schemas to match drt-core on every PR that touches
`drt/config/**`, so a pending release is simply "schemas changed since the last
published version". Check the diff since the last `vscode-drt` version in
`CHANGELOG.md`.

## Per-release steps

1. From a repo checkout with the `schema-gen` extra
   (`pip install -e ".[schema-gen]"`), regenerate and confirm the bundle is
   current:

   ```bash
   integrations/vscode-drt/scripts/regenerate-schemas.sh
   git diff --stat integrations/vscode-drt/schemas
   ```

2. Bump `version` in `integrations/vscode-drt/package.json`.
3. In `integrations/vscode-drt/CHANGELOG.md`, date the new section and note the
   drt-core version the schemas were generated from. The CHANGELOG is bundled
   into the `.vsix`, so do this **before** packaging.
4. Merge that change to `main`.
5. Build the package (Node 18+):

   ```bash
   cd integrations/vscode-drt
   npx --yes @vscode/vsce package --out ~/Downloads/vscode-drt-<version>.vsix
   ```

   The package name is `@vscode/vsce`. The listing should contain only
   `LICENSE`, `changelog.md`, `icon.png`, `package.json`, `readme.md` and the two
   schemas (~30 KB); anything else means `.vscodeignore` needs a line.
6. **VS Code Marketplace:** sign in at <https://marketplace.visualstudio.com/manage>
   with the publisher's Microsoft account, open publisher `drt-hub`, and either
   use **New extension → Visual Studio Code** (first release) or the extension's
   `…` menu → **Update** (later releases). Upload the `.vsix`. Status shows
   "Verifying…" for a few minutes, then the new version number.
7. **Open VSX:** publish the same `.vsix` with
   `npx --yes ovsx publish ~/Downloads/vscode-drt-<version>.vsix -p <token>`
   (token from <https://open-vsx.org/user-settings/tokens>; namespace `drt-hub`).
8. Verify the listings:
   - <https://marketplace.visualstudio.com/items?itemName=drt-hub.vscode-drt>
   - <https://open-vsx.org/extension/drt-hub/vscode-drt>
9. Tag the release for the record: `git tag vscode-drt-v<version> && git push origin vscode-drt-v<version>`
   (no workflow listens to this tag yet).

## Accounts

- Marketplace publisher `drt-hub` is owned by the project Microsoft account
  (drt.hub.dev@gmail.com). Its publisher ID is permanent.
- Open VSX uses the maintainer's GitHub login, linked to an Eclipse Foundation
  account, with the Publisher Agreement signed and namespace `drt-hub`.
- Never commit tokens. Pass the Open VSX token on the command line or via the
  `OVSX_PAT` environment variable.

## Automating later

If an Azure subscription is ever available, replace steps 6-7 with a
`vscode-drt-v*` tag-triggered workflow running `vsce publish` (secret
`VSCE_PAT`) and `ovsx publish` (secret `OVSX_PAT`), mirroring
`publish-dagster-drt.yml`.
