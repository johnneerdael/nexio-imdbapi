# VitePress Docs Site Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a dedicated VitePress docs site under `docs-site/` and deploy it to the repository's default GitHub Pages URL with comprehensive documentation for the API, frontend portal, and self-hosting story.

**Architecture:** Keep `docs/` as the source material and contract/document-generation area, while `docs-site/` becomes the public static docs app. Build the VitePress site from curated Markdown, wire a dark custom theme layer, and deploy only the static output with GitHub Pages Actions.

**Tech Stack:** VitePress, Markdown, GitHub Actions Pages workflow, existing npm workspace/root package scripts

---

### Task 1: Scaffold The VitePress App

**Files:**
- Create: `docs-site/.vitepress/config.ts`
- Create: `docs-site/.vitepress/theme/custom.css`
- Create: `docs-site/index.md`
- Modify: `package.json`

- [ ] **Step 1: Write the failing test**

Check that the repo does not yet have a VitePress app or docs-site build script.

Run: `test -f docs-site/.vitepress/config.ts; echo $?`
Expected: `1`

- [ ] **Step 2: Run test to verify it fails**

Run: `npm run docs:build`
Expected: failure because `docs:build` does not exist yet

- [ ] **Step 3: Write minimal implementation**

Add the VitePress config, custom dark theme CSS, a landing page, and root scripts for local dev/build.

- [ ] **Step 4: Run test to verify it passes**

Run: `npm run docs:build`
Expected: VitePress build succeeds and emits static output

- [ ] **Step 5: Commit**

```bash
git add package.json docs-site/.vitepress/config.ts docs-site/.vitepress/theme/custom.css docs-site/index.md
git commit -m "docs: scaffold vitepress docs site"
```

### Task 2: Author Core Documentation Content

**Files:**
- Create: `docs-site/guide/getting-started.md`
- Create: `docs-site/api/overview.md`
- Create: `docs-site/api/authentication.md`
- Create: `docs-site/api/bulk-jobs.md`
- Create: `docs-site/frontend/portal.md`
- Create: `docs-site/frontend/google-auth.md`
- Create: `docs-site/self-hosting/overview.md`
- Create: `docs-site/self-hosting/docker-compose.md`
- Create: `docs-site/self-hosting/proxies.md`
- Create: `docs-site/security/secrets.md`
- Create: `docs-site/operations/runbook.md`

- [ ] **Step 1: Write the failing test**

Check that the expected content files do not exist yet.

Run: `find docs-site -maxdepth 2 -type f | rg 'getting-started|authentication|docker-compose|runbook'`
Expected: no matches

- [ ] **Step 2: Run test to verify it fails**

Run: `npm run docs:build`
Expected: build either fails on missing links/navigation targets or succeeds without the required content pages

- [ ] **Step 3: Write minimal implementation**

Create the section pages with detailed explanations covering API usage, portal architecture, Google auth setup, secrets, self-hosting, and operations.

- [ ] **Step 4: Run test to verify it passes**

Run: `npm run docs:build`
Expected: build succeeds with all section pages rendered

- [ ] **Step 5: Commit**

```bash
git add docs-site
git commit -m "docs: add vitepress product and deployment content"
```

### Task 3: Add GitHub Pages Deployment Workflow

**Files:**
- Create: `.github/workflows/docs-pages.yml`
- Modify: `README.md`

- [ ] **Step 1: Write the failing test**

Check that the workflow file does not exist yet.

Run: `test -f .github/workflows/docs-pages.yml; echo $?`
Expected: `1`

- [ ] **Step 2: Run test to verify it fails**

Run: `find .github/workflows -maxdepth 1 -type f | rg 'docs-pages'`
Expected: no matches

- [ ] **Step 3: Write minimal implementation**

Add a GitHub Pages workflow using `actions/configure-pages`, `actions/upload-pages-artifact`, and `actions/deploy-pages`, and update the README to point at the docs site.

- [ ] **Step 4: Run test to verify it passes**

Run: `sed -n '1,240p' .github/workflows/docs-pages.yml`
Expected: workflow includes build and deploy jobs for the VitePress output

- [ ] **Step 5: Commit**

```bash
git add .github/workflows/docs-pages.yml README.md
git commit -m "ci: deploy vitepress docs to github pages"
```

### Task 4: Verify The Docs Site End To End

**Files:**
- Test: `docs-site/**`
- Test: `.github/workflows/docs-pages.yml`
- Test: `package.json`

- [ ] **Step 1: Write the failing test**

Run the VitePress build before any final fixes.

Run: `npm run docs:build`
Expected: identify any remaining broken links, config issues, or asset path problems

- [ ] **Step 2: Run test to verify it fails**

Confirm the exact failure if any exists.

- [ ] **Step 3: Write minimal implementation**

Fix any broken links, navigation problems, or workflow/config mismatches discovered during the build.

- [ ] **Step 4: Run test to verify it passes**

Run:
- `npm run docs:build`
- `npm run build:docs`

Expected:
- VitePress build succeeds
- API Blueprint HTML generation still succeeds

- [ ] **Step 5: Commit**

```bash
git add package.json docs-site .github/workflows/docs-pages.yml README.md
git commit -m "docs: finalize vitepress pages site"
```
