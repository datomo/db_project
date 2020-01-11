import Vue from 'vue'
import Vuex from 'vuex'

Vue.use(Vuex)

export default new Vuex.Store({
  state: {
    showZips: false
  },
  mutations: {
    toggleShowZips (state) {
      state.showZips = !state.showZips
    }
  },
  actions: {
  },
  modules: {
  },
  getters: {
    get_show_zip: state => {
      return state.showZips
    }
  }
})
