import { createApp } from 'vue'
import { createPinia } from 'pinia'
import { library } from '@fortawesome/fontawesome-svg-core'
import {
  faArrowLeft,
  faArrowUpRightFromSquare,
  faBolt,
  faBrain,
  faBuilding,
  faChartLine,
  faCheckCircle,
  faCircleCheck,
  faClock,
  faClipboardCheck,
  faCloudArrowUp,
  faCog,
  faCopy,
  faComments,
  faDatabase,
  faDownload,
  faEdit,
  faEllipsisVertical,
  faExclamationCircle,
  faFile,
  faFileArrowUp,
  faFileExcel,
  faFileLines,
  faFilePdf,
  faFilter,
  faFolder,
  faFolderOpen,
  faGears,
  faInfoCircle,
  faListCheck,
  faMagnifyingGlass,
  faPaperPlane,
  faPlay,
  faPlus,
  faRefresh,
  faRedo,
  faRotate,
  faRotateRight,
  faShieldHalved,
  faSpinner,
  faTimesCircle,
  faTrash,
  faTriangleExclamation,
  faUpload,
  faXmark,
  faScissors,
  faLayerGroup
} from '@fortawesome/free-solid-svg-icons'
import { FontAwesomeIcon } from '@fortawesome/vue-fontawesome'

import App from './App.vue'
import router from './router'
import './assets/styles/main.css'

library.add(
  faArrowLeft,
  faArrowUpRightFromSquare,
  faBolt,
  faBrain,
  faBuilding,
  faChartLine,
  faCheckCircle,
  faCircleCheck,
  faClock,
  faClipboardCheck,
  faCloudArrowUp,
  faCog,
  faCopy,
  faComments,
  faDatabase,
  faDownload,
  faEdit,
  faEllipsisVertical,
  faExclamationCircle,
  faFile,
  faFileArrowUp,
  faFileExcel,
  faFileLines,
  faFilePdf,
  faFilter,
  faFolder,
  faFolderOpen,
  faGears,
  faInfoCircle,
  faListCheck,
  faMagnifyingGlass,
  faPaperPlane,
  faPlay,
  faPlus,
  faRefresh,
  faRedo,
  faRotate,
  faRotateRight,
  faShieldHalved,
  faSpinner,
  faTimesCircle,
  faTrash,
  faTriangleExclamation,
  faUpload,
  faXmark,
  faScissors,
  faLayerGroup
)

const app = createApp(App)

app.use(createPinia())
app.use(router)
app.component('FontAwesomeIcon', FontAwesomeIcon)

app.mount('#app')
