<template>
  <div class="combined h-auto w-full flex items-center justify-around">
    <db-combined-map :zip_1="crime"
                     :zip_2="review"
                     title="Crime & Reviews"
                     low_color="rgb(255, 255, 83)"
                     high_color="rgb(0, 0, 83)"/>
    <db-combined-map :zip_1="crime" :zip_2="pill"
                     title="Crime & Pill"
                     low_color="rgb(255, 255, 83)"
                     high_color="rgb(0, 0, 83)"
                     scala="Conformity"/>
    <db-combined-map :zip_1="review"
                     :zip_2="pill"
                     title="Review & Pill"
                     low_color="rgb(255, 255, 83)"
                     high_color="rgb(0, 0, 83)"
                     scala="Conformity"/>
  </div>
</template>

<script>
import dbCombinedMap from '@/components/CombinedMap.vue'
import crime from '@/assets/crime.json'
import review from '@/assets/review_filtered.json'
import pill from '@/assets/pill.json'

export default {
  name: 'Combined',
  data () {
    return {
      crime: crime,
      review: review,
      pill: pill
    }
  },
  components: {
    dbCombinedMap
  }
}
</script>
