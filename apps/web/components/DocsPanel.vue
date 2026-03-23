<template>
  <section class="surface rounded-[32px] p-4 md:p-5">
    <div class="px-3 py-3 md:px-5">
      <span class="badge">API Blueprint</span>
      <h2 class="section-title text-3xl font-extrabold mt-4">Contract documentation</h2>
      <p class="text-soft mt-3">
        Generated HTML is served when available; otherwise the raw blueprint contract is returned.
      </p>
    </div>
    <div class="overflow-hidden rounded-[28px] border border-white/10 mt-4 bg-black/30">
      <iframe src="/api/docs" class="w-full min-h-[900px] bg-[#0b0b0b]" title="API documentation"></iframe>
    </div>
  </section>
</template>
