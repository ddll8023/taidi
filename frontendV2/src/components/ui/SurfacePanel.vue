<script setup>
/**
 * 通用卡片容器
 * 功能描述：统一页面中的信息块承载样式
 * 依赖组件：无
 */
defineProps({
  title: {
    type: String,
    default: ''
  },
  description: {
    type: String,
    default: ''
  },
  eyebrow: {
    type: String,
    default: ''
  },
  padded: {
    type: Boolean,
    default: true
  }
})
</script>

<template>
  <section class="shell-panel" :class="padded ? 'p-5 sm:p-6' : ''">
    <header v-if="title || description || eyebrow" class="mb-5">
      <p v-if="eyebrow" class="shell-kicker">{{ eyebrow }}</p>
      <h3 v-if="title" class="mt-2 text-lg font-semibold text-ink-900">{{ title }}</h3>
      <p v-if="description" class="mt-2 text-sm leading-6 text-ink-600">{{ description }}</p>
    </header>
    <slot />
  </section>
</template>
