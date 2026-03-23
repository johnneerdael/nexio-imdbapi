# VitePress Docs Site Design

## Goal

Add a dedicated VitePress documentation site under `docs-site/` and publish it to the repository's default GitHub Pages URL. The site should present a professional dark visual style and document the API, frontend portal, and self-hosting story in one coherent place without coupling the Pages build to the Nuxt application.

## Scope

This design covers:

- a standalone VitePress app under `docs-site/`
- detailed documentation content for API, frontend, self-hosting, security, and operations
- a GitHub Actions workflow that deploys the VitePress build to GitHub Pages
- root package scripts and README links needed to work with the docs site

This design does not replace:

- `docs/api.apib` as the API contract source of truth
- the Nuxt portal's internal docs behavior
- the existing deployment guides under `docs/`

## Architecture

The repo keeps `docs/` as the authoritative source for API Blueprint and deployment/source material, while `docs-site/` becomes the reader-facing documentation product. VitePress builds a static site from curated Markdown pages that summarize and organize the operational knowledge already present in the repo, with links back to the Blueprint HTML and source guides where needed.

GitHub Pages deployment is isolated to the static docs site. The workflow builds only the VitePress site and deploys the generated output via the current GitHub Pages Actions flow. The VitePress config sets `base` dynamically for default repository Pages hosting so asset paths work under `https://<user>.github.io/<repo>/`.

## Information Architecture

The docs site will include:

- landing page
- getting started
- API overview and auth
- endpoint categories and bulk-job behavior
- frontend portal architecture and Google auth flow
- secrets and environment variables
- self-hosting with Docker Compose and proxy choices
- operations, troubleshooting, upgrades, and backups

The navigation should favor operational readability:

- top nav for major product areas
- sidebars per section
- strong cross-links between auth, env vars, deployment, and troubleshooting

## Visual Direction

The site should default to dark mode and feel intentional rather than boilerplate:

- graphite and slate surfaces
- high-contrast typography
- restrained accent color
- soft gradients and depth on the landing page
- compact cards for major doc entry points

This should remain VitePress-native rather than becoming a custom app.

## Content Strategy

The VitePress pages should not duplicate raw API Blueprint detail line-for-line. Instead:

- explain the product model and operational concepts in prose
- summarize endpoint groups and usage patterns
- point to the Blueprint-generated docs for contract-level details
- document secrets, Google auth setup, and deployment sequences step by step

## Deployment Model

The GitHub Pages workflow should:

- trigger on pushes to the default branch and manual dispatch
- install root dependencies
- build the VitePress site
- upload the static artifact
- deploy using `actions/deploy-pages`

The workflow should not require application secrets because GitHub Pages deployment uses the repo-scoped `GITHUB_TOKEN`.

## Risks And Constraints

- The repo already has docs content in multiple places, so the new site must avoid drifting away from the source guides.
- GitHub Pages hosts static output only, so the site must avoid runtime dependencies on the Nuxt portal or backend.
- The Pages base path must be correct for default repo hosting or assets will break.

## Acceptance Criteria

- `docs-site/` exists as a standalone VitePress app
- the site builds locally from the repo root
- a GitHub Actions workflow deploys it to GitHub Pages
- the content covers API, frontend, self-hosting, secrets, Google auth, and operations in detail
- the site uses a dark visual treatment by default
