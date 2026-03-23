<script setup lang="ts">
const session = ref<{ authenticated: boolean; user: Record<string, unknown> } | null>(null)

try {
  session.value = await $fetch('/auth/session')
} catch {
  session.value = null
}
</script>

<template>
  <PortalChrome>
    <template #actions>
      <NuxtLink class="ghost-btn" to="/">Portal</NuxtLink>
    </template>

    <template v-if="!session">
      <AuthCard />
    </template>

    <template v-else>
      <DocsPanel />
    </template>
  </PortalChrome>
</template>
