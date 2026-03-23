<template>
  <div class="portal-page">
    <div class="portal-aurora" aria-hidden="true" />
    <div class="portal-shell px-4 py-6 md:px-6 lg:px-8">
      <header class="glass rounded-[28px] px-5 py-4 md:px-7 md:py-5 flex flex-col gap-4 md:flex-row md:items-center md:justify-between">
        <div class="flex items-center gap-3">
          <div class="h-11 w-11 rounded-2xl bg-gradient-to-br from-primary to-primary-dim flex items-center justify-center text-black font-black">
            N
          </div>
          <div>
            <div class="text-xs uppercase tracking-[0.18em] text-soft">Nexio Internal</div>
            <div class="section-title text-xl font-extrabold">IMDb API Portal</div>
            <div class="text-xs text-soft mt-1">Google-authenticated. Postgres-backed. Dark by design.</div>
          </div>
        </div>

        <div class="flex items-center gap-3">
          <slot name="actions" />
        </div>
      </header>

      <main class="mt-6">
        <slot />
      </main>
    </div>
  </div>
</template>
