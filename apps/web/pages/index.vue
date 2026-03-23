<script setup lang="ts">
type BootstrapPayload = {
  user: {
    id: string
    email: string
    displayName: string | null
    avatarUrl: string | null
  }
  snapshot: {
    id: string
    dataset_name: string
    source_url: string | null
    notes: string
    imported_at: string
    completed_at: string | null
    is_active: boolean
    title_count: number
    name_count: number
    rating_count: number
    status: 'active' | 'staged'
  } | null
  stats: {
    title_count: number
    rating_count: number
    episode_count: number
    name_count: number
  }
  apiKeys: Array<{
    id: string
    key_prefix: string
    label: string
    created_at: string
    last_used_at: string | null
    revoked_at: string | null
  }>
}

const session = ref<{ authenticated: boolean; user: Record<string, unknown> } | null>(null)
const bootstrap = ref<BootstrapPayload | null>(null)
const createdKey = ref<string | null>(null)
const error = ref<string | null>(null)

async function loadSession() {
  try {
    session.value = await $fetch('/auth/session')
  } catch {
    session.value = null
  }
}

async function loadBootstrap() {
  if (!session.value) {
    bootstrap.value = null
    return
  }

  bootstrap.value = await $fetch('/api/portal/bootstrap')
}

async function createKey(label: string) {
  error.value = null
  try {
    const payload = await $fetch<{ apiKey: string }>('/api/portal/keys/create', {
      method: 'POST',
      body: { label }
    })
    createdKey.value = payload.apiKey
    await loadBootstrap()
  } catch (caught) {
    error.value = caught instanceof Error ? caught.message : 'Failed to create API key.'
  }
}

async function revokeKey(id: string) {
  error.value = null
  try {
    await $fetch(`/api/portal/keys/${id}/revoke`, { method: 'POST' })
    await loadBootstrap()
  } catch (caught) {
    error.value = caught instanceof Error ? caught.message : 'Failed to revoke API key.'
  }
}

async function signOut() {
  await $fetch('/auth/logout', { method: 'POST' })
  session.value = null
  bootstrap.value = null
}

await loadSession()
if (session.value) {
  await loadBootstrap()
}
</script>

<template>
  <PortalChrome>
    <template #actions>
      <template v-if="session && bootstrap">
        <div class="text-right mr-2 hidden md:block">
          <div class="text-sm text-soft">{{ bootstrap.user.displayName || bootstrap.user.email }}</div>
          <div class="text-xs uppercase tracking-[0.12em] text-soft/70">{{ bootstrap.user.email }}</div>
        </div>
        <NuxtLink class="ghost-btn" to="/docs">API Documentation</NuxtLink>
        <button class="ghost-btn" @click="signOut">Sign out</button>
      </template>
    </template>

    <template v-if="!session">
      <AuthCard />
    </template>

    <template v-else-if="bootstrap">
      <div class="grid gap-6">
        <StatsOverview :snapshot="bootstrap.snapshot" :stats="bootstrap.stats" />

        <section v-if="createdKey" class="glass rounded-[28px] p-6 border border-[#86f7c9]/20">
          <span class="badge">Copy now</span>
          <h2 class="section-title text-2xl font-extrabold mt-4">New API key</h2>
          <p class="text-soft mt-3">This value is only shown once. The backend stores only its hash.</p>
          <pre class="mt-5 rounded-[22px] bg-black/40 p-4 overflow-x-auto text-sm">{{ createdKey }}</pre>
        </section>

        <section v-if="error" class="glass rounded-[28px] p-5 text-[#ffd7da] bg-[rgba(80,12,18,0.45)]">
          {{ error }}
        </section>

        <ApiKeysPanel :items="bootstrap.apiKeys" @create="createKey" @revoke="revokeKey" />
        <DocsPanel />
      </div>
    </template>
  </PortalChrome>
</template>
