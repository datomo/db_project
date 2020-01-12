<template>
  <div class="combined-map">
    <db-map :title="title"
            :zips="this.zips"
            :low_color="low_color"
            :high_color="high_color"
            text_color="black"
            scala="Conformity"/>
  </div>
</template>

<script>
import dbMap from '@/components/Map.vue'

export default {
  name: 'TripleMap',
  props: ['zip_1', 'zip_2', 'zip_3', 'title', 'low_color', 'high_color', 'flip'],
  components: {
    dbMap
  },
  data () {
    return {
      zips: {}
    }
  },
  async mounted () {
    // all available zips
    let flip = this.flip
    if (!flip) {
      flip = [false, false, false]
    }
    const zips = [...new Set(Object.keys(this.zip_2).concat(Object.keys(this.zip_1), Object.keys(this.zip_3)))]
    const temp = {}
    await zips.forEach(async (zip) => {
      if (this.zip_1[zip] && this.zip_2[zip] && this.zip_3[zip] && zip !== '') {
        const zip1 = this.zip_1[zip]
        const zip2 = this.zip_2[zip]
        const zip3 = this.zip_3[zip]
        temp[zip] = ((flip[0] ? 1 - zip1 : zip1) + (flip[1] ? 1 - zip2 : zip2) + (flip[2] ? 1 - zip3 : zip3)) / 3
      }
    })
    this.zips = temp
  }
}
</script>

<style scoped>

</style>
