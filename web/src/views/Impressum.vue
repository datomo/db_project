<template>
    <div class="impressum w-full h-full flex">
      <div class="content shadow-md p-12 w-auto mx-auto my-auto bg-white items-center justify-center">
        <p class="pb-4">This webpage is a part of the database lesson at unversity Basel.</p>
        <p class="font-bold">Resources</p>
        <ul class="list-inside list-disc">
          <li class="hover:underline"><a href="https://vuejs.org/" target="_blank">Vue</a ></li>
            <ul class="ml-4 list-inside">
              <li>To create the logic for this interactive visualisation</li>
            </ul>
          <li class="hover:underline"><a href="https://tailwindcss.com/" target="_blank">tailwindcss</a></li>
            <ul class="ml-4 list-inside">
              <li>To speed up the designing and layouting of this page</li>
            </ul>
          <li class="hover:underline"><a href="https://github.com/timmywil/panzoom" target="_blank">Panzoom</a></li>
          <li class="hover:underline"><a href="https://www.npmjs.com/package/color-between" target="_blank">colorBetween</a></li>
          <li class="hover:underline"><a href="https://arizona.hometownlocator.com/zip-codes/zipcodes,city,phoenix.cfm" target="_blank">Phoenix Zip Map</a> </li>
          <ul class="ml-4 list-inside">
            <li>To use as a base for the interactive heatmaps</li>
          </ul>
        </ul>
      </div>

    </div>
</template>

<script>
export default {
  name: 'Impressum'
}
</script>

<style scoped>

</style>
