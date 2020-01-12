<template>
  <div class="combined-map">
    <db-map :title="title"
            subtitle="Standard Deviation"
            :zips="this.zips"
            :low_color="low_color"
            :high_color="high_color"
            :text_color="text_color"
            scala="[0,1]"/>
  </div>
</template>

<script>
import dbMap from '@/components/Map.vue'

export default {
  name: 'TripleMap',
  props: ['zip_1', 'zip_2', 'zip_3', 'title', 'low_color', 'high_color', 'flip', 'text_color'],
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
      let zip1 = this.zip_1[zip]
      let zip2 = this.zip_2[zip]
      let zip3 = this.zip_3[zip]
      if (zip1 !== undefined && zip2 !== undefined && zip3 !== undefined && zip !== '') {
        zip1 = flip[0] ? 1 - zip1 : zip1
        zip2 = flip[1] ? 1 - zip2 : zip2
        zip3 = flip[2] ? 1 - zip3 : zip3
        const mean = (zip1 + zip2 + zip3) / 3
        const std = Math.sqrt((Math.pow((zip1 - mean), 2) + Math.pow((zip2 - mean), 2) + Math.pow((zip3 - mean), 2)) / 2)
        temp[zip] = std
      }
    })
    const max = Math.max.apply(null, Object.values(temp))
    const min = Math.min.apply(null, Object.values(temp))
    Object.keys(temp).forEach(async (zip) => {
      temp[zip] = 1 - (temp[zip] - min) / (max - min)
    })
    this.zips = temp
  }
}
</script>

<style scoped>

</style>
