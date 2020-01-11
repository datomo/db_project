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
  name: 'CombinedMap.vue',
  props: ['zip_1', 'zip_2', 'title', 'low_color', 'high_color'],
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
    const zips = [...new Set(Object.keys(this.zip_2).concat(Object.keys(this.zip_1)))]
    console.log(zips)
    const temp = {}
    await zips.forEach(async (zip) => {
      if (this.zip_1[zip] && this.zip_2[zip] && zip !== '') {
        const ratio = (this.zip_1[zip] + this.zip_2[zip]) / 2
        temp[zip] = ratio
      }
    })
    this.zips = temp
  }
}
</script>

<style scoped>

</style>
