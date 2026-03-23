---
layout: home
title: Nexio IMDb docs
hero:
  name: Nexio IMDb docs
  text: Internal API, portal, and self-hosting guidance for the IMDb platform.
  tagline: Built with VitePress and deployed to GitHub Pages.
  actions:
    - theme: brand
      text: Start here
      link: '#what-this-site-covers'
    - theme: alt
      text: Source code
      link: https://github.com/johnneerdael/nexio-imdbapi
features:
  - title: API contract
    details: Keep authentication, bulk jobs, and endpoint behavior close to the implementation.
  - title: Portal notes
    details: Document how the Nuxt portal, Google auth, and API key flows fit together.
  - title: Self-hosting
    details: Collect docker-compose, proxy, and secret-management guidance in one place.
---

## What this site covers

This docs site is the public entry point for the platform documentation. It is
meant to stay lightweight and readable while still covering the areas people
need most during development, review, and deployment.

The source material still lives in `docs/`, while this site curates the
platform-facing material into a separate VitePress app that can be deployed
independently.

## Next steps

The first published sections will cover the API surface, the internal portal,
and the self-hosting story. The navigation and theme are already scaffolded so
the documentation can grow without changing the site structure.
