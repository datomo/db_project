<template>
  <div class="combined-map">
    <db-map :title="title"
            :zips="this.zips"
            :zip_values="this.values"
            :low_color="low_color"
            :high_color="high_color"
            text_color="black"
            scala="Similarity[-1,1]"/>
  </div>
</template>

<script>
import dbMap from '@/components/Map.vue'

export default {
  name: 'CombinedMap.vue',
  props: ['zip_1', 'zip_2', 'title', 'low_color', 'high_color', 'flip'],
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
      flip = [false, false]
    }
    const zips = Object.keys(this.zip_2).filter(element => Object.keys(this.zip_1).includes(element))
    const temp = {}
    await zips.forEach(async (zip) => {
      const zip1 = this.zip_1[zip]
      const zip2 = this.zip_2[zip]
      if (zip in this.zip_1 && zip in this.zip_2 && zip !== '') {
        const ratio = (1 - Math.abs(((!flip[0] ? zip1 : (1 - zip1)) - (!flip[1] ? zip2 : (1 - zip2)))))
        if (!isNaN(ratio)) temp[zip] = ratio
      }
    })
    this.values = {}
    await Object.keys(temp).forEach(zip => {
      this.values[zip] = (temp[zip] * 2) - 1
    })
    this.zips = temp
  }
}
</script>

<style scoped>

</style>
