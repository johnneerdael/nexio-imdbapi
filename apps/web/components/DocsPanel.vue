<template>
  <section class="surface rounded-[32px] p-6 md:p-7">
    <div class="flex flex-col gap-6 lg:flex-row lg:items-end lg:justify-between">
      <div class="max-w-3xl">
        <span class="badge">API Blueprint</span>
        <h2 class="section-title text-3xl font-extrabold mt-4">Contract documentation</h2>
        <p class="text-soft mt-3">
          The full Aglio-rendered reference now lives on its own wide route with a three-column layout, so examples and schema panels are readable without an embedded scrollbox.
        </p>
      </div>

      <div class="flex flex-wrap gap-3">
        <NuxtLink class="primary-btn" to="/docs">Open API documentation</NuxtLink>
        <a class="ghost-btn" href="/api/docs" target="_blank" rel="noreferrer">Open raw generated page</a>
      </div>
    </div>

    <div class="mt-6 grid gap-4 md:grid-cols-3">
      <div class="glass rounded-[24px] p-5">
        <div class="text-xs uppercase tracking-[0.18em] text-soft">Layout</div>
        <div class="section-title text-xl font-extrabold mt-3">Three-column reading flow</div>
        <p class="text-soft mt-3">Navigation, endpoint narrative, and live examples each keep their own column.</p>
      </div>

      <div class="glass rounded-[24px] p-5">
        <div class="text-xs uppercase tracking-[0.18em] text-soft">Theme</div>
        <div class="section-title text-xl font-extrabold mt-3">Matched to the portal</div>
        <p class="text-soft mt-3">Typography, background gradients, chips, and code surfaces now align with the rest of the Nexio UI.</p>
      </div>

      <div class="glass rounded-[24px] p-5">
        <div class="text-xs uppercase tracking-[0.18em] text-soft">Source</div>
        <div class="section-title text-xl font-extrabold mt-3">Generated from `api.apib`</div>
        <p class="text-soft mt-3">The API Blueprint contract remains the source of truth and still renders through Aglio at build time.</p>
      </div>
    </div>
  </section>
</template>
