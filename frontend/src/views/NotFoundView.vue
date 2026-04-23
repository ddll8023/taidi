<script setup>
/**
 * 404 页面
 * 功能描述：未命中路由时返回当前项目入口
 * 依赖组件：无
 */
</script>

<template>
  <div class="flex min-h-screen items-center justify-center px-6 py-12">
    <div class="shell-panel max-w-xl p-8 text-center">
      <p class="shell-kicker">404</p>
      <h1 class="mt-3 text-3xl font-semibold tracking-tight text-ink-900">页面不存在</h1>
      <p class="mt-4 text-sm leading-7 text-ink-600">
        当前地址没有对应的前端页面，请返回记录中心或上传页继续操作。
      </p>
      <div class="mt-6 flex flex-wrap justify-center gap-3">
        <RouterLink to="/reports/list" class="shell-button">返回记录中心</RouterLink>
      </div>
    </div>
  </div>
</template>
