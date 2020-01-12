<template>
    <div class="final h-auto w-full flex items-center justify-around">
      <db-triple-map :zip_1="crime"
                     :zip_2="pill"
                     :zip_3="review"
                     title="Crime & Pill & Review (worst)"
                     :flip="[false, false, true]"
                     low_color="rgb(255, 255, 83)"
                     high_color="rgb(0, 0, 83)"/>
    </div>
</template>

<script>
import dbTripleMap from '@/components/TripleMap.vue'
import crime from '@/assets/crime.json'
import review from '@/assets/review_filtered.json'
import pill from '@/assets/pill.json'

export default {
  name: 'Final',
  components: {
    dbTripleMap
  },
  data () {
    return {
      crime: crime,
      review: review,
      pill: pill
    }
  }
}
</script>

<style scoped>

</style>
