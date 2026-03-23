<script setup lang="ts">
defineProps<{
  snapshot: Record<string, unknown> | null
  stats: {
    title_count: number
    rating_count: number
    episode_count: number
    name_count: number
  }
}>()
</script>

<template>
  <section class="grid gap-5 lg:grid-cols-[1.2fr_0.8fr]">
    <article class="surface rounded-[32px] p-7 md:p-8">
      <div class="flex items-start justify-between gap-4">
        <div>
          <span class="badge">Snapshot</span>
          <h2 class="section-title text-3xl font-extrabold mt-4">Dataset state</h2>
        </div>
        <div class="text-right text-sm text-soft">
          <div>Status</div>
          <div class="text-white font-semibold mt-1">{{ snapshot?.status || 'Unavailable' }}</div>
        </div>
      </div>

      <dl class="mt-8 grid gap-4 md:grid-cols-2">
        <div class="glass rounded-[24px] p-5">
          <dt class="text-xs uppercase tracking-[0.14em] text-soft">Imported</dt>
          <dd class="mt-2 text-lg font-semibold">{{ snapshot?.imported_at || 'n/a' }}</dd>
        </div>
        <div class="glass rounded-[24px] p-5">
          <dt class="text-xs uppercase tracking-[0.14em] text-soft">Upstream updated</dt>
          <dd class="mt-2 text-lg font-semibold">{{ snapshot?.source_updated_at || 'n/a' }}</dd>
        </div>
      </dl>
    </article>

    <article class="surface rounded-[32px] p-7 md:p-8">
      <span class="badge">Footprint</span>
      <div class="mt-5 grid gap-4">
        <div class="glass rounded-[22px] p-4">
          <div class="text-xs uppercase tracking-[0.14em] text-soft">Titles</div>
          <div class="text-2xl font-black mt-2">{{ stats.title_count.toLocaleString() }}</div>
        </div>
        <div class="glass rounded-[22px] p-4">
          <div class="text-xs uppercase tracking-[0.14em] text-soft">Ratings</div>
          <div class="text-2xl font-black mt-2">{{ stats.rating_count.toLocaleString() }}</div>
        </div>
        <div class="glass rounded-[22px] p-4">
          <div class="text-xs uppercase tracking-[0.14em] text-soft">Episodes</div>
          <div class="text-2xl font-black mt-2">{{ stats.episode_count.toLocaleString() }}</div>
        </div>
        <div class="glass rounded-[22px] p-4">
          <div class="text-xs uppercase tracking-[0.14em] text-soft">Names</div>
          <div class="text-2xl font-black mt-2">{{ stats.name_count.toLocaleString() }}</div>
        </div>
      </div>
    </article>
  </section>
</template>
