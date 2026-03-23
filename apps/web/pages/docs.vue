<script setup lang="ts">
const requestFetch = useRequestFetch()
const session = ref<{ authenticated: boolean; user: Record<string, unknown> } | null>(null)

try {
  session.value = await requestFetch('/auth/session')
} catch {
  session.value = null
}
</script>

<template>
  <PortalChrome wide>
    <template #actions>
      <NuxtLink class="ghost-btn" to="/">Portal</NuxtLink>
    </template>

    <template v-if="!session">
      <AuthCard />
    </template>

    <template v-else>
      <section class="surface rounded-[32px] p-4 md:p-5">
        <div class="px-3 py-3 md:px-5 flex flex-col gap-5 lg:flex-row lg:items-end lg:justify-between">
          <div class="max-w-3xl">
            <span class="badge">API Documentation</span>
            <h1 class="section-title text-4xl md:text-5xl font-black mt-4">Full contract reference</h1>
            <p class="text-soft text-lg leading-relaxed mt-4">
              This view serves the generated Aglio output in a dedicated wide canvas so endpoint navigation, request examples, and response bodies stay readable on large screens.
            </p>
          </div>

          <div class="flex flex-wrap gap-3">
            <a class="secondary-btn" href="/api/docs" target="_blank" rel="noreferrer">Open generated HTML</a>
            <NuxtLink class="ghost-btn" to="/">Back to portal</NuxtLink>
          </div>
        </div>

        <div class="mt-5 overflow-hidden rounded-[28px] border border-white/10 bg-black/30">
          <iframe
            src="/api/docs"
            class="w-full min-h-[calc(100vh-270px)] bg-[#0b0b0b]"
            title="API documentation"
          />
        </div>
      </section>
    </template>
  </PortalChrome>
</template>
