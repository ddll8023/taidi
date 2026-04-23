<script setup>
/**
 * 项目侧边栏
 * 功能描述：展示极简项目导航与当前模块入口
 * 设计风格：简约清新风格，白色背景，蓝色强调
 * 依赖组件：无
 */
import { computed, ref } from 'vue'
import { useRoute } from 'vue-router'

import { navigationGroups } from '@/data/navigation'

const route = useRoute()

const navigationItems = computed(() => navigationGroups.flatMap((group) => group.items))

const isActive = (targetPath) => {
  if (route.path === targetPath) return true
  
  const hasMoreSpecificMatch = navigationItems.value.some(
    item => item.to !== targetPath && 
            item.to.startsWith(`${targetPath}/`) && 
            (route.path === item.to || route.path.startsWith(`${item.to}/`))
  )
  
  if (hasMoreSpecificMatch) return false
  
  return route.path.startsWith(`${targetPath}/`)
}

const hoveredItem = ref(null)

const getNavItemClasses = (to) => {
  const active = isActive(to)
  const hovered = hoveredItem.value === to
  
  return {
    'relative flex flex-col items-center justify-center gap-2 rounded-xl px-3 py-3 transition-all duration-200 cursor-pointer group': true,
    'bg-accent-50 border border-accent-200': active,
    'hover:bg-ink-50': !active,
    'border border-transparent': !active
  }
}

const getIconWrapperClasses = (to) => {
  const active = isActive(to)
  
  return {
    'relative flex items-center justify-center w-11 h-11 rounded-xl transition-all duration-200': true,
    'bg-accent-500 shadow-soft': active,
    'bg-ink-100 group-hover:bg-ink-200': !active
  }
}

const getIconClasses = (to) => {
  const active = isActive(to)
  
  return {
    'text-base transition-all duration-200': true,
    'text-white': active,
    'text-ink-600 group-hover:text-ink-700': !active
  }
}

const getLabelClasses = (to) => {
  const active = isActive(to)
  
  return {
    'text-xs font-medium transition-all duration-200 whitespace-nowrap': true,
    'text-accent-600': active,
    'text-ink-500 group-hover:text-ink-600': !active
  }
}
</script>

<template>
  <aside class="fixed left-0 top-0 z-30 hidden h-screen w-[88px] flex-col border-r border-ink-200 bg-white md:flex">
    <div class="flex flex-1 flex-col items-center gap-3 overflow-y-auto px-3 py-5">
      <div class="flex flex-col items-center gap-6">
        <RouterLink
          to="/reports/list"
          class="group relative flex flex-col items-center gap-1.5 transition-all duration-200"
          title="财报智能分析"
        >
          <div class="relative flex h-12 w-12 items-center justify-center rounded-xl bg-accent-500 shadow-soft transition-all duration-200 group-hover:shadow-glow-sm group-hover:scale-105">
            <FontAwesomeIcon :icon="['fas', 'chart-line']" class="text-lg text-white" />
          </div>
          <div class="flex flex-col items-center gap-0.5">
            <span class="text-[9px] font-bold uppercase tracking-wider text-accent-500">
              FRA
            </span>
            <span class="text-[10px] font-medium text-ink-600">
              财报
            </span>
          </div>
        </RouterLink>

        <div class="h-px w-10 bg-ink-200"></div>

        <nav class="flex flex-col items-center gap-1.5">
          <RouterLink
            v-for="item in navigationItems"
            :key="item.to"
            :to="item.to"
            :class="getNavItemClasses(item.to)"
            :title="item.title"
            @mouseenter="hoveredItem = item.to"
            @mouseleave="hoveredItem = null"
          >
            <div :class="getIconWrapperClasses(item.to)">
              <FontAwesomeIcon
                :icon="['fas', item.icon]"
                :class="getIconClasses(item.to)"
              />
            </div>
            
            <span :class="getLabelClasses(item.to)">
              {{ item.shortLabel || item.title }}
            </span>
            
            <div
              v-if="isActive(item.to)"
              class="absolute left-0 top-1/2 h-6 w-0.5 -translate-y-1/2 -translate-x-1/2 rounded-full bg-accent-500"
            ></div>
          </RouterLink>
        </nav>
      </div>

    </div>
  </aside>

  <aside class="fixed bottom-0 left-0 right-0 z-30 border-t border-ink-200 bg-white md:hidden">
    <nav class="flex items-center justify-around gap-2 px-3 py-2">
      <RouterLink
        v-for="item in navigationItems"
        :key="item.to"
        :to="item.to"
        class="group flex flex-1 flex-col items-center justify-center gap-1 rounded-xl px-2 py-2 transition-all duration-200"
        :class="isActive(item.to) ? 'bg-accent-50' : ''"
        :title="item.title"
      >
        <div
          class="flex h-9 w-9 items-center justify-center rounded-xl transition-all duration-200"
          :class="isActive(item.to) ? 'bg-accent-500 shadow-soft' : 'bg-ink-100 group-hover:bg-ink-200'"
        >
          <FontAwesomeIcon
            :icon="['fas', item.icon]"
            class="text-sm transition-colors duration-200"
            :class="isActive(item.to) ? 'text-white' : 'text-ink-600 group-hover:text-ink-700'"
          />
        </div>
        <span
          class="text-[10px] font-medium transition-colors duration-200"
          :class="isActive(item.to) ? 'text-accent-600' : 'text-ink-500 group-hover:text-ink-600'"
        >
          {{ item.shortLabel || item.title }}
        </span>
      </RouterLink>
    </nav>
  </aside>
</template>
