<template>
  <div id="app" class="h-screen w-screen bg-gray-200 flex">
    <div id="nav" class="flex flex-col items-center justify-center align-center px-8 py-2 shadow-md bg-white m-4">
      <router-link class="whitespace-no-wrap hover:underline" to="/">Base Maps</router-link>
      <h1>-</h1>
      <router-link class="whitespace-no-wrap hover:underline" to="/combined">Combined Maps</router-link>
      <h1>-</h1>
      <router-link class="whitespace-no-wrap hover:underline" to="/final">Final Map</router-link>

    </div>
    <div class="wrapper flex flex-col flex-grow">
      <h1 class="text-3xl font-bold p-3 pb-0 w-full text-center">Database Project Group 4 <span class="text-lg">Isabel G. & David L.</span></h1>
      <h1 class="text-lg bold p-1 pt-0 w-full text-center italic">Is there a correlation between drug usage, crime rate and yelp reviews in Phoenix Arizona?</h1>
      <hr>
      <router-view class="p-4"/>
      <div class="flex-grow"></div>
      <div class="copyright w-full flex p-2 text-sm text-gray-800 justify-end">
        <router-link class="whitespace-no-wrap hover:underline" to="/impressum">Impressum ©{{new Date().getFullYear()}} Isabel Geissmann & David Lengweiler</router-link>
      </div>
    </div>
  </div>
</template>

<style lang="scss">

  @tailwind base;

  @tailwind components;

  @tailwind utilities;

  h1 {
    @apply text-xl;
  }

  .router-link-exact-active{
    @apply font-bold;
  }
</style>
