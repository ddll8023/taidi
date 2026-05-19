import { createApp } from 'vue'
import { createPinia } from 'pinia'
import { library } from '@fortawesome/fontawesome-svg-core'
import {
  faBuilding,
  faChartLine,
  faFileExcel,
  faFileLines,
  faCloudArrowUp,
  faRotateRight,
  faTrash,
  faCheckCircle,
  faTimesCircle,
  faSpinner,
  faFilePdf,
  faXmark,
  faArrowUpRightFromSquare,
  faPlay
} from '@fortawesome/free-solid-svg-icons'
import { FontAwesomeIcon } from '@fortawesome/vue-fontawesome'

import App from './App.vue'
import router from './router'
import './assets/styles/main.css'

library.add(
  faBuilding,
  faChartLine,
  faFileExcel,
  faFileLines,
  faCloudArrowUp,
  faRotateRight,
  faTrash,
  faCheckCircle,
  faTimesCircle,
  faSpinner,
  faFilePdf,
  faXmark,
  faArrowUpRightFromSquare,
  faPlay
)

const app = createApp(App)

app.use(createPinia())
app.use(router)
app.component('FontAwesomeIcon', FontAwesomeIcon)

app.mount('#app')
