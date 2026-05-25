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
  faPlay,
  faMicrochip,
  faArrowLeft,
  faSearch,
  faChevronDown,
  faChevronRight,
  faRobot,
  faUser,
  faPaperPlane,
  faStop,
  faCopy,
  faCheck,
  faMessage,
  faComments,
  faPlus,
  faTriangleExclamation,
  faExclamationCircle,
  faGear,
  faUpload,
  faScissors,
  faBolt,
  faCubes,
  faList,
  faInbox,
  faAnglesLeft,
  faAngleLeft,
  faAngleRight,
  faAnglesRight,
  faChartPie
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
  faPlay,
  faMicrochip,
  faArrowLeft,
  faSearch,
  faChevronDown,
  faChevronRight,
  faRobot,
  faUser,
  faPaperPlane,
  faStop,
  faCopy,
  faCheck,
  faMessage,
  faComments,
  faPlus,
  faTriangleExclamation,
  faExclamationCircle,
  faGear,
  faUpload,
  faScissors,
  faBolt,
  faCubes,
  faList,
  faInbox,
  faAnglesLeft,
  faAngleLeft,
  faAngleRight,
  faAnglesRight,
  faChartPie
)

const app = createApp(App)

app.use(createPinia())
app.use(router)
app.component('FontAwesomeIcon', FontAwesomeIcon)

app.mount('#app')
