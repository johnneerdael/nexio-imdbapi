<template>
  <section class="surface rounded-[32px] p-8 md:p-10 max-w-3xl mx-auto">
    <span class="badge">Google Auth</span>
    <h1 class="section-title text-4xl md:text-5xl font-black leading-[1.02] mt-5">
      Internal IMDb infrastructure, without external auth glue.
    </h1>
    <p class="text-soft text-lg leading-relaxed mt-5 max-w-2xl">
      Sign in with an approved Google account to view dataset status, read API documentation, and mint scoped API keys for internal clients.
    </p>

    <div class="mt-8 grid gap-4 md:grid-cols-[1.2fr_0.8fr]">
      <div class="glass rounded-[28px] p-6">
        <div class="text-sm uppercase tracking-[0.16em] text-soft">What you get</div>
        <ul class="mt-4 space-y-3 text-[0.96rem] text-soft">
          <li>Direct Google OIDC sign-in handled in Nuxt/Nitro.</li>
          <li>Postgres-backed sessions and one-time-visible API keys.</li>
          <li>Live snapshot visibility plus the full API Blueprint contract.</li>
        </ul>
      </div>

      <div class="surface rounded-[28px] p-6 flex flex-col justify-between">
        <div>
          <div class="text-sm uppercase tracking-[0.16em] text-soft">Access</div>
          <p class="text-soft leading-relaxed mt-4">
            Only email addresses present in `ALLOWED_GOOGLE_EMAILS` can enter the portal.
          </p>
        </div>
        <a class="primary-btn text-center mt-6" href="/auth/google?next=/">Continue with Google</a>
      </div>
    </div>
  </section>
</template>

