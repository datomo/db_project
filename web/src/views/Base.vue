<template>
  <div class="home h-auto w-full flex items-center justify-around">
    <db-map title="Crime"
            :ratio="crimePop"
            :zips="this.crime"
            :population="this.population"
            low_color="rgb(189, 189, 242)"
            high_color="rgb(0, 0, 255)"
            scala="Crime Rate[0,1]"/>
    <db-map title="Review"
            text_color="black"
            :zips="this.review"
            :population="this.population"
            low_color="rgb(255, 255, 255)"
            high_color="rgb(254, 3, 3)"
            scala="AVG Stars[0,1]"/>
    <db-map title="Pill"
            text_color="black"
            :zips="this.pill"
            :population="this.population"
            low_color="rgb(255, 255, 194)"
            high_color="rgb(39, 104, 15)"
            scala="Buy Amount[0,1]"/>
  </div>
</template>

<script>
// @ is an alias to /src
import dbMap from '@/components/Map.vue'
import crime from '@/assets/crime.json'
import population from '@/assets/population.json'
import crimePop from '@/assets/crime_population.json'
import review from '@/assets/review_filtered.json'
import pill from '@/assets/pill.json'

export default {
  name: 'home',
  data () {
    return {
      crime: crime,
      population: population,
      crimePop: crimePop,
      review: review,
      pill: pill
    }
  },
  components: {
    dbMap
  }
}
</script>
