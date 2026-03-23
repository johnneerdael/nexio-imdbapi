<script setup lang="ts">
type ApiKeyRow = {
  id: string
  key_prefix: string
  label: string
  created_at: string
  last_used_at: string | null
  revoked_at: string | null
}

const props = defineProps<{
  items: ApiKeyRow[]
}>()

const emit = defineEmits<{
  create: [label: string]
  revoke: [id: string]
}>()

const modalOpen = ref(false)
const label = ref('Android TV client')

function submit() {
  emit('create', label.value)
}
</script>

<template>
  <section class="surface rounded-[32px] p-7 md:p-8">
    <div class="flex flex-col gap-4 md:flex-row md:items-start md:justify-between">
      <div>
        <span class="badge">API Keys</span>
        <h2 class="section-title text-3xl font-extrabold mt-4">Mint access tokens</h2>
        <p class="text-soft mt-3 max-w-2xl">
          Keys are hashed at rest and shown only once at creation time.
        </p>
      </div>
      <button class="primary-btn" @click="modalOpen = true">Generate key</button>
    </div>

    <div class="mt-7 grid gap-4">
      <article
        v-for="item in props.items"
        :key="item.id"
        class="glass rounded-[24px] p-5 flex flex-col gap-4 md:flex-row md:items-center md:justify-between"
      >
        <div>
          <div class="text-sm uppercase tracking-[0.12em] text-soft">{{ item.key_prefix }}</div>
          <div class="font-semibold mt-2">{{ item.label }}</div>
          <div class="text-sm text-soft mt-2">
            Created {{ item.created_at }}<span v-if="item.last_used_at"> • Last used {{ item.last_used_at }}</span>
          </div>
        </div>
        <div class="flex items-center gap-3">
          <span class="badge">{{ item.revoked_at ? 'Revoked' : 'Active' }}</span>
          <button v-if="!item.revoked_at" class="ghost-btn" @click="emit('revoke', item.id)">Revoke</button>
        </div>
      </article>
    </div>

    <div v-if="modalOpen" class="fixed inset-0 z-50 bg-black/70 backdrop-blur-md flex items-center justify-center px-4">
      <div class="surface rounded-[32px] p-7 w-full max-w-lg">
        <div class="flex items-center justify-between gap-3">
          <div>
            <div class="badge">New key</div>
            <h3 class="section-title text-2xl font-extrabold mt-4">Generate API key</h3>
          </div>
          <button class="ghost-btn" @click="modalOpen = false">Close</button>
        </div>

        <label class="field-shell mt-6">
          <span class="text-soft">Label</span>
          <input v-model="label" type="text" placeholder="Android TV client" />
        </label>

        <div class="mt-6 flex justify-end gap-3">
          <button class="ghost-btn" @click="modalOpen = false">Cancel</button>
          <button class="primary-btn" @click="submit">Create</button>
        </div>
      </div>
    </div>
  </section>
</template>

