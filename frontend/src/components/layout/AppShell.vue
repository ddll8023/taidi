<script setup>
/**
 * 应用布局壳
 * 功能描述：承接项目侧边栏与页面内容区
 * 设计风格：简约清新风格，适配新的侧边栏宽度
 * 依赖组件：AppSidebar
 */
import AppSidebar from '@/components/layout/AppSidebar.vue'
</script>

<template>
  <div class="h-screen bg-transparent text-ink-900">
    <div class="relative flex h-full w-full flex-col md:flex-row">
      <AppSidebar />

      <div class="relative flex flex-1 min-w-0 flex-col md:pl-[88px]">
        <main class="flex flex-1 min-w-0 flex-col overflow-x-hidden px-4 pt-6 sm:px-5 lg:px-6">
          <RouterView class="min-h-0 min-w-0" />
        </main>
      </div>
    </div>
  </div>
</template>
