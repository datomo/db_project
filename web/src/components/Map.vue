<template>
  <div class="w-auto h-auto flex flex p-3">
    <div class="mx-auto inline-block w-auto mr-2">
      <div class=" shadow-md bg-white rounded p-2 ">
        <div class="flex items-center">
          <label class="pr-2" for="showZips">Show Zips</label>
          <input id="showZips" type="checkbox" v-model="showZips">
        </div>
        <div class="flex items-center justify-center">
          <label class="pr-2" for="showZips">Show Values</label>
          <input id="showPercent" type="checkbox" v-model="showPercent">
        </div>
        <button class="border rounded p-1 my-1 shadow-md" @click="el.reset()">reset</button>
      </div>
      <div class="shadow-md bg-white rounded mt-4 p-2 flex flex-col justify-center items-center">
        <p class="font-semibold">{{scala}}</p>
        <p>lowest</p>
        <svg width="100" height="100">
          <linearGradient :id="'gradient' + title.replace(/ /g, '').replace(/&/g, '')" x1="0%" y1="0%" x2="0%" y2="100%">
            <stop offset="0%" :style="{stopColor: low_color}"/>
            <stop offset="100%" :style="{stopColor: high_color}"/>
          </linearGradient>
          <path d="M 10 10 H 90 V 90 H 10 L 10 10" :fill="'url(#gradient' + title.replace(/ /g, '').replace(/&/g, '') + ')'"/>
        </svg>
        <p>highest</p>
      </div>
      <div class="shadow-md bg-white rounded mt-4 p-2 flex flex-col justify-center items-center">
        <p class="pb-2 font-bold">No Data</p>
        <svg width="50" height="50">
          <rect height="50" width="50" style="fill:rgb(255,255,255);stroke-width:1;stroke:rgb(0,0,0)" />
        </svg>
      </div>
    </div>
    <div class="shadow-md bg-white rounded">
      <h1 class="text-center pt-4 font-semibold">{{title}}</h1>
      <div ref="parent">
        <div :id="'map-' + title.replace(/ /g, '').replace(/&/g, '')" class="svg-container overflow-hidden overflow-hidden">
          <svg class="w-full pb-8 p-8 h-full flex-0"  xmlns="http://www.w3.org/2000/svg"
               viewBox="0 0 291 725.3">
            <title>{{title.replace(/ /g, '').replace(/&/g, '')}}</title>
            <g ref="Layer_1" data-name="Layer 1">
              <path fill="#fff" ref="_85003" data-name="85003" class="cls-1"
                    d="M176.5,524.8v-27l3-1,2,1,1,4-3,1,3,1v6l-3,1v2l3,1v15h-2v2l2,1-1,10,2,6-7,3v-26Z"/>
              <path fill="#fff" ref="_85004" data-name="85004" class="cls-1"
                    d="M189.5,504.8v35h-2l2,1-3,2v2h3v4l-5-1-3-9,1-6-2-4h2v-15h-3v-3h3v-7h-3l3-1-1-6h5v2l1-2h2v4l-3,1v2Z"/>
              <path fill="#fff" ref="_85006" data-name="85006" class="cls-1"
                    d="M201.5,522.8h-12v-18l-3-1v-2h3l1-5h3v4h2v-2l4-2h2l3,3,6-3,4,3v23h-13Z"/>
              <path fill="#fff" ref="_85007" data-name="85007" class="cls-1"
                    d="M164.5,557.8l-1-10h5v-2h-5v-7h2v-2h-2v-39l12-1,2,51-1,3-7,6h-3l1-2-3,2Z"/>
              <path fill="#fff" ref="_85008" data-name="85008" class="cls-1"
                    d="M243.5,524.8l-3-2-1,3-8-1,1-2h-18v-25l3-1,1,3,2-3h2l1,2h4l1-2,1,2v-2h2v2l1-2h10v2h2v-2h2v4l1-3,2-1,2,1,1,3h3v0h2l1-3,7,1v5h12l-1,15,1,4h3l-4,8-6-6h-6v3l-5-1v0l-10-1v-2l-2-1h-2v4l-1-1Z"/>
              <path fill="#fff" ref="_85009" data-name="85009" class="cls-1"
                    d="M138.5,556.8h-6v5l-12-2-7,1v-11h2v-2h11v-28h-2l-1-3,3-1v-8h-2l2-4v-5h-3l5-2h7v2h2v-2l1,2,1-2h2v3h2v-3h2v2l6,1v-2l2-1h4v2l2-2h4v40h2v2h-2v7h5v2h-5l1,10-3,1-22-2Z"/>
              <path fill="#fff" ref="_85012" data-name="85012" class="cls-1"
                    d="M182.5,471.8v-24l7-1v8l-2,1v2l2,1v16h-2l2,4v13l-3,1h3l-3,1v2l3,1h-2v2l-1-2h-4v-3l-3-2,1-6h2l-3-1v-2h3l-2-2,2-3h-2l2-1h-3l3-1v-4Z"/>
              <path fill="#fff" ref="_85013" data-name="85013" class="cls-1"
                    d="M181.5,477.8l1,2h-2v2h2l-3,3,3,1h-2l-1,6,3,1v3l-3,2h-8v-51l11,1v28h-3Z"/>
              <path fill="#fff" ref="_85015" data-name="85015" class="cls-1"
                    d="M160.5,496.8l-2,2-1-2h-2l-1-4v-47h9v2l3,1,2-2,3,4-1,46h-10Z"/>
              <path fill="#fff" ref="_85016" data-name="85016" class="cls-1"
                    d="M210.5,496.8h-2v2l-5,1-2-3-1-22,2-19h-2v-3h2v-6l9-1,5,2-2-1v-13l13-1v34l-2,2v3l1-1,1,2v4h-2l2,1v10h-2v3h2v2l-2,1v3h2v2h-4v-2h-2l-2,3-3-3-2,1v2l-3-3Z"/>
              <path fill="#fff" ref="_85017" data-name="85017" class="cls-1"
                    d="M145.5,496.8h-2v3h-2v-3h-2v2l-1-16-3-2,2-1-2-1,2-1,1-3v-27h-2l5-1v3l1-3,9-1,1,2,1-2,1,51h-3v3h-3v-2l-3,1Z"/>
              <path fill="#fff" ref="_85018" data-name="85018" class="cls-1"
                    d="M240.5,445.8l-1,5-11,1-1-19h12v9Zm-13,45h-2v-2l2-1v-9l-2-1,2-1v-6h-2l2-6,8,2,2,2,3-1v-8l2-2,5,2,7-1,1,2h3v5h9l5,7-1,18h2v4l-11,4-5-1v3h-2v0h-3v-3l-5-1v4l-1-4h-2v2h-2v-2h-10v2l-1-2h-2l-1,2-1-2h-2v-3h2v-3Z"/>
              <path fill="#fff" ref="_85019" data-name="85019" class="cls-1"
                    d="M135.5,478.8h2l-2,1,3,4v13l-3,2v-2h-9v-49h12v30h-3Z"/>
              <path fill="#fff" ref="_85020" data-name="85020" class="cls-1"
                    d="M195.5,446.8h-13v-22l-4-4h4l-1-8,4-2-1-3h3l1-2v-8l2-2-1-10,5,1,9,7,4,1,2,3-1,22,2,5-2,3v5h6v14h-19Z"/>
              <path fill="#fff" ref="_85021" data-name="85021" class="cls-1"
                    d="M171.5,446.8h-3l-2,2-3-1v-2h-9v-17l-3-4v-16h2l1,9,3-2v-6h2l1-2h13l3,2v-2h8l1,3-4,2,1,8h-3l3,4v22h-11Z"/>
              <path fill="#fff" ref="_85022" data-name="85022" class="cls-1"
                    d="M201.5,344.8h2v3l2-2,5-1v3l4,2-2,1v2h2v12h-3l3,5-1,7,4-1v2h2l-1,4h2l-2,1v2h-2l-2,11-4,1h-2l-1-2h-2l-12-9h-4v2h-3l-2,4-3-2h-6l7-1v-9l2-1-2-3-1-7,2-7-3,2-1-2v-1h3v-16h19Z"/>
              <path fill="#fff" ref="_85023" data-name="85023" class="cls-1"
                    d="M174.5,385.8l-3,1-6-3-14-1,1-31,2-7,7,1,1,3,2-4h17l1,15-3,1v3l4-2-2,8,2,3-1,3,2,2-2,1v7h-8Z"/>
              <path fill="#fff" ref="_85024" data-name="85024" class="cls-1"
                    d="M195.5,344.8h-11v-6h-3l1-6,6-1v-13h6v-2l5-5-4-1-5-6-2,2h-12v-13l1-1,10,2,1,7h3l3-2v-6h5l2-2v-6h5l1-3,5-1h17l-12,24h5v3h4l-1-1,1,11h-5l-2-2-6-1v6l2,2,11,2h-13l1,7,3,1v2l-1-1-2,2,3,1v2l-3-1v6h-3v3l-1-2-5-1-1,3-2-3Z"/>
              <path fill="#fff" ref="_85027" data-name="85027" class="cls-1"
                    d="M153.5,344.8l-15,1v-52h13l1,9h5v-3l19,1v6h12l2-2,4,6h5l-2,4-3,2v2h-6v13l-6,1-1,6h2v6h-19l-2,4-1-3Z"/>
              <path fill="#fff" ref="_85028" data-name="85028" class="cls-1"
                    d="M208.5,428.8v-3l2-1-2-5v-23l8-1v2h3l1-3h21l6,3,5-1,1,29-4,1-1-2h-2l-1,8h-36Z"/>
              <path fill="#fff" ref="_85029" data-name="85029" class="cls-1"
                    d="M157.5,409.8l1,6-3,2-2-3v-6h-6l-2,2-7-2h-12v-26h15l-2,4,3-1v-3h5v3l3-1,1,5v-7h14l6,4,11-1v2l-2,1h-5l9,3,2-4h3l1,8-2,2-1,9-10,1-3,2-14-2-1,2Z"/>
              <path fill="#fff" ref="_85031" data-name="85031" class="cls-1"
                    d="M121.5,496.8v3h-2v-3h-19v-5l7-2-7-2v-15l26-2v26Z"/>
              <path fill="#fff" ref="_85032" data-name="85032" class="cls-1"
                    d="M250.5,396.8l-2,1-7-3h-18l-3,1-1,2h-3v-2h-2v-8l2-3h2v-2h2l-2-3,1-2h-2v-2l-4,1,1-9-1-2h-2l3-1v-12h-2l2-4h-3v-3l28-2v2l1-2h13v13h2l-3,3v21h2v2h-2v14Z"/>
              <path fill="#fff" ref="_85033" data-name="85033" class="cls-1"
                    d="M92.5,496.8h-17l-1,5-4-1-1-3h-6v-25l37-1v15l7,4h-7v6Z"/>
              <path fill="#fff" ref="_85034" data-name="85034" class="cls-1"
                    d="M240.5,522.8l5,4v-4h4v3l6,1h4v0l5,1v6l-12,1v13h-14l-2,1v3h-2l-6,5-3-2-2-4-19,2-14-4-1-4h-3v-2l3-1-2-1,2-1-1-9,1-7,2-1h41l-1,1,8,2v-2Z"/>
              <path fill="#fff" ref="_85035" data-name="85035" class="cls-1"
                    d="M94.5,511.8l-25,1v-2l-5-1v3l-1-15h5l4,4,3-1v-4h44v3h2v-3h2v2h3v8h0v6h-12l-1-3h-6v2h-13Z"/>
              <path fill="#fff" ref="_85037" data-name="85037" class="cls-1"
                    d="M63.5,506.8v4h-13l-2,2h-6l-4-2v-25H1.5l-1-12h13v-7l6,1v-4h3v-3h2l1,12h8l1-2h3v2l26-1v35Z"/>
              <path fill="#fff" ref="_85040" data-name="85040" class="cls-1"
                    d="M211.5,572.8l-25,3-1-2h-5v-3h2l1-3v-18h-4v2h-3l5-4,19,3,4,2,19-2,5,6,2-3,6-2v-3l16-1-1,25-21,2-5-2h-14Z"/>
              <path fill="#fff" ref="_85041" data-name="85041" class="cls-1"
                    d="M169.5,605.8v6l-4-2-2,1v5h-12v-30l-20,2-5-1v-19l-6,2-3-1-2,2-2-10,7-1,12,2v-5l32,2v-2h2l1-2-1,2h2l6-5h5v-2h4v21h-3v3h3v12l-4,1h4v4l-2,2,2,11-2,2h-12Z"/>
              <path fill="#fff" ref="_85042" data-name="85042" class="cls-1"
                    d="M179.5,586.8l3-1,1-2v-10h2v2l26-3h14l8,2,18-2,1,15-9-1-1,3,3-1,2,1,1,3h5v6h2l-6,1-1,3-10,5v11l-12-1v10l-13-8-8,1-4-4h-4v2h-7l-3,4-3,1,1,3h-2l-1,3-2-3-16-1-1-17v2l2-1,4,2v-6h12l2-2v-8l-2-4,2-1v-4h-4Z"/>
              <path fill="#fff" ref="_85043" data-name="85043" class="cls-1"
                    d="M64.5,567.8h-2l2-8-2-13,1-12h12v-13h-12v-8l2-3-1-1h5v2l6,1,32-1v-2h6l1,3h12v3l-3,1,1,3h2v28h-11v2h-2v11h-3v2l-4,2,1,2h-7l1,2-5,1h-8v-3l-5,2-2-1v-3h-3v2h-14Z"/>
              <path fill="#fff" ref="_85044" data-name="85044" class="cls-1"
                    d="M228.5,642.8l-3,2-1-2,2-2-1-1h2l-3-3,2-7v-12l12,1v-11h3l-1-1,8-4v-2l3-1-1-1h3v-6h-5l-1-3-2-1-3,1v-3l9,1,1-2h7v15l-3,11v38h-27l6-3-4-4Z"/>
              <path fill="#fff" ref="_85045" data-name="85045" class="cls-1"
                    d="M176.5,659.8l-2,2-9,1-1,47-22-15-13-26-1-6,4-9-6-7v-9h6l3,3h3l4-3h21v12l14-1,4,2-4,4v4Zm5-16v3l-3,3-2-2v-6h4Z"/>
              <path fill="#fff" ref="_85048" data-name="85048" class="cls-1"
                    d="M203.5,711.8l-1,12-15,1-22-15-1-2v-45h10l3-4v-4l4-4-18-1v-24h12l6,1,1,2,3-2-1-3,3-1,3-4h6l4-2,5,4,8-1,1,2,4,1,7,5,1,2-2,7,3,3-1-1-2,4h6l5,4-6,2,1,1h26v12l-17,1v24h-2l-1,2v2h3l1,7-1,14-3-3-9-3h-13v6h-11Zm-26-63,1,1,3-3v-2l-1-3-4-1v8Z"/>
              <path fill="#fff" ref="_85050" data-name="85050" class="cls-1"
                    d="M243.5,281.8h8l2,10,6,11v6l-6,11v2l6,2-1,3-5-1v17l-7,2-5-1-1,2-1-2-25,1v-6l3,1v-2h-3v-2h2l1-2-3-1-1-6,13-1-11-2-2-2v-5l3-1,5,3h5v-11l-1,2-3-1,2-2-7-1,13-24h13Z"/>
              <path fill="#fff" ref="_85051" data-name="85051" class="cls-1"
                    d="M138.5,408.8l8,2,5-2v16l3,6-1,17-2-2h-9v4l-1-3h-12l-1,2-2-1v-39h12Z"/>
              <path fill="#fff" ref="_85053" data-name="85053" class="cls-1"
                    d="M138.5,352.8v-6l3-2h12l-2,22v23l-1-5-3,1v-3h-5v3l-3,1,2-4h-15v-25h12v-5Z"/>
              <path fill="#fff" ref="_85054" data-name="85054" class="cls-1"
                    d="M266.5,348.8l-1-12-8-4h-4v-7l5,1,1-3-6-2,6-13v-5l-7-17,1-18h6l5,8,1,5h25v77l-12-3-11-7Z"/>
              <path fill="#fff" ref="_85083" data-name="85083" class="cls-1"
                    d="M103.5,249.8l1,1,8-2,1,6h6l1-1-2-2-3-10-3-3v-10l16,3,2-13h2v4h6l12,71h-11l-3-6h-2v4l-4,2-42,1v-44l4-5,10,4Z"/>
              <path fill="#fff" ref="_85085" data-name="85085" class="cls-1"
                    d="M157.5,299.8v3h-5l-15-84h28l4-2,1,3,3-2h49l1,3-5,3-4,8-6,6,3,6h9l-6,6,4,2,3,5-1,2,2,2-5,21h-10l-1,4h-5v6l-2,2h-5v6l-2,2h-4v-5l-2-3h-9l-1,7Z"/>
              <path fill="#fff" ref="_85086" data-name="85086" class="cls-1"
                    d="M131.5,218.8l-3,7v6h-4l-6-3h-6v10l3,3,3,10,2,2-7,1v-6l-9,2-3-1-3-3,6-6-5-5-2,1-3-10-4-3-1-4h-12l-1-4,2-2-12-1v-3h-3l-1-3,3-13h13v7h7v-4h-5v-3l-15-1,1-6,18,2,10-14,6,6h9v-3l-3-5,5-5v-4l3-2,4-7-2,2,9-2v-3l4-1v-2h9v5l8,1h68v31h2l-2,5v27h-41l-3,2-1-3-4,2h-27l1,7-1-3h-6v-4Z"/>
              <path fill="#fff" ref="_85087" data-name="85087" class="cls-1"
                    d="M118.5,154.8l-6,9-1,4-5,4,3,9h-9l-6-5-5-1h-14v-12l-7,11-11-2-1-1,4-4h2v-36l-16-1,3-4h11l-1-1,2-1v-2l-3-4,2-2h5l2-18-3-3-4-1,4-4v-2l-3-2v-4l1-1,7,2,5-6,6-3-1-3,3-1v-2l6-1V9.8l7,2,8,5v5l2,2,5-2,5,3,1,4,3-5,3,1,3,6,3-3,2-14-2-5V.8l2,1v18l8,14,1,8,6,4,5-1,10,3,3-1-1-3,2-4,13-11,5-11,7-3v27h47v100h3v13h-101v-6h-9v2h-4v4l-9,2Z"/>
              <path fill="#fff" ref="_85014" data-name="85014" class="cls-1"
                    d="M187.5,455.8h2l1-8,12-1v6h-2v2l2,1-2,19,1,22-5,1-1,3h-2v-4h-3v2l-1-3h-3v-2h3l-3-2h3v-13l-2-2v-2h2v-16l-2-1Z"/>
            </g>
            <g ref="Layer_2" :style="{fill: text_color }" data-name="Layer 2" v-show="showPercent">
              <text class="cls-2" ref="percent_85003" transform="translate(169.77 532.2)"></text>
              <text class="cls-2" ref="percent_85004" transform="translate(175.77 519.35)"></text>
              <text class="cls-2" ref="percent_85006" transform="matrix(1 0 0 1 193.2676 512.7998)"></text>
              <text class="cls-2" ref="percent_85008" transform="translate(235.89 515.11)"></text>
              <text class="cls-2" ref="percent_85034" transform="translate(212.19 538.13)"></text>
              <text class="cls-2" ref="percent_85040" transform="translate(207.86 565.24)"></text>
              <text class="cls-2" ref="percent_85041" transform="translate(145.89 579.59)"></text>
              <text class="cls-2" ref="percent_85042" transform="translate(203.77 597.59)"></text>
              <text class="cls-2" ref="percent_85044" transform="translate(232.27 629.8)"></text>
              <text class="cls-2" ref="percent_85048" transform="translate(194.88 672.1)"></text>
              <text class="cls-2" ref="percent_85045" transform="translate(138.77 662.3)"></text>
              <text class="cls-2" ref="percent_85027" transform="translate(151.77 318.18)"></text>
              <text class="cls-2" ref="percent_85085" transform="translate(167.77 259.98)"></text>
              <text class="cls-2" ref="percent_85054" transform="translate(264.27 321.48)"></text>
              <text class="cls-2" ref="percent_85050" transform="translate(225.27 335.83)"></text>
              <text class="cls-2" ref="percent_85024" transform="translate(191.27 339.9)"></text>
              <text class="cls-2" ref="percent_85083" transform="translate(105.27 273.18)"></text>
              <text class="cls-2" ref="percent_85032" transform="translate(225.27 373.27)"></text>
              <text class="cls-2" ref="percent_85022" transform="translate(190.27 378.97)"></text>
              <text class="cls-2" ref="percent_85023" transform="translate(159.27 367.48)"></text>
              <text class="cls-2" ref="percent_85053" transform="translate(127.77 370.8)"></text>
              <text class="cls-2" ref="percent_85028" transform="translate(219.77 413.8)"></text>
              <text class="cls-2" ref="percent_85020" transform="translate(184.77 425.57)"></text>
              <text class="cls-2" ref="percent_85029" transform="translate(140.77 397.8)"></text>
              <text class="cls-2" ref="percent_85051" transform="translate(131.27 432.8)"></text>
              <text class="cls-2" ref="percent_85021" transform="translate(159.27 435.9)"></text>
              <text class="cls-2" ref="percent_85087" transform="translate(131.27 96.98)"></text>
              <text class="cls-2" ref="percent_85018" transform="translate(240.27 482.68)"></text>
              <text class="cls-2" ref="percent_85016" transform="translate(203.77 470.8)"></text>
              <text class="cls-2" ref="percent_85014" transform="translate(183.94 463.85)"></text>
              <text class="cls-2" ref="percent_85012" transform="translate(174.82 469.54)"></text>
              <text class="cls-2" ref="percent_85015" transform="translate(153.27 475.01)"></text>
              <text class="cls-2" ref="percent_85013" transform="translate(166.77 482.68)"></text>
              <text class="cls-2" ref="percent_85017" transform="translate(135.77 461.11)"></text>
              <text class="cls-2" ref="percent_85019" transform="translate(121.74 466.3)"></text>
              <text class="cls-2" ref="percent_85037" transform="translate(31.96 482.68)"></text>
              <text class="cls-2" ref="percent_85033" transform="translate(70.93 486.8)"></text>
              <text class="cls-2" ref="percent_85031" transform="translate(103.77 482.68)"></text>
              <text class="cls-2" ref="percent_85035" transform="translate(85.27 504.8)"></text>
              <text class="cls-2" ref="percent_85043" transform="translate(88.24 533.11)"></text>
              <text class="cls-2" ref="percent_85009" transform="translate(134.95 528.56)"></text>
              <text class="cls-2" ref="percent_85007" transform="translate(159.27 525.37)"></text>
              <text class="cls-2" ref="percent_85086" transform="translate(144.27 192.71)"></text>
            </g>
            <g ref="Layer_3" :style="{fill: text_color }" data-name="Layer 3" v-show="showZips">
              <text class="cls-2" ref="text_85003" transform="translate(169.77 532.2)">85003</text>
              <text class="cls-2" ref="text_85004" transform="translate(175.77 519.35)">85004</text>
              <text class="cls-2" ref="text_85006" transform="matrix(1 0 0 1 193.2676 512.7998)">85006</text>
              <text class="cls-2" ref="text_85008" transform="translate(235.89 515.11)">85008</text>
              <text class="cls-2" ref="text_85034" transform="translate(212.19 538.13)">85034</text>
              <text class="cls-2" ref="text_85040" transform="translate(207.86 565.24)">85040</text>
              <text class="cls-2" ref="text_85041" transform="translate(145.89 579.59)">85041</text>
              <text class="cls-2" ref="text_85042" transform="translate(203.77 597.59)">85042</text>
              <text class="cls-2" ref="text_85044" transform="translate(232.27 629.8)">85044</text>
              <text class="cls-2" ref="text_85048" transform="translate(194.88 672.1)">85048</text>
              <text class="cls-2" ref="text_85045" transform="translate(138.77 662.3)">85045</text>
              <text class="cls-2" ref="text_85027" transform="translate(151.77 318.18)">85027</text>
              <text class="cls-2" ref="text_85085" transform="translate(167.77 259.98)">85085</text>
              <text class="cls-2" ref="text_85054" transform="translate(264.27 321.48)">85054</text>
              <text class="cls-2" ref="text_85050" transform="translate(225.27 335.83)">85050</text>
              <text class="cls-2" ref="text_85024" transform="translate(191.27 339.9)">85024</text>
              <text class="cls-2" ref="text_85083" transform="translate(105.27 273.18)">85083</text>
              <text class="cls-2" ref="text_85086" transform="translate(144.27 192.71)">85086</text>
              <text class="cls-2" ref="text_85087" transform="translate(131.27 96.98)">85087</text>
              <text class="cls-2" ref="text_85032" transform="translate(225.27 373.27)">85032</text>
              <text class="cls-2" ref="text_85022" transform="translate(190.27 378.97)">85022</text>
              <text class="cls-2" ref="text_85023" transform="translate(159.27 367.48)">85023</text>
              <text class="cls-2" ref="text_85053" transform="translate(127.77 370.8)">85053</text>
              <text class="cls-2" ref="text_85053" transform="translate(127.77 370.8)">85053</text>
              <text class="cls-2" ref="text_85028" transform="translate(219.77 413.8)">85028</text>
              <text class="cls-2" ref="text_85020" transform="translate(184.77 425.57)">85020</text>
              <text class="cls-2" ref="text_85029" transform="translate(140.77 397.8)">85029</text>
              <text class="cls-2" ref="text_85051" transform="translate(131.27 432.8)">85051</text>
              <text class="cls-2" ref="text_85021" transform="translate(159.27 435.9)">85021</text>
              <text class="cls-2" ref="text_85018" transform="translate(240.27 482.68)">85018</text>
              <text class="cls-2" ref="text_85016" transform="translate(203.77 470.8)">85016</text>
              <text class="cls-2" ref="text_85014" transform="translate(183.94 463.85)">85014</text>
              <text class="cls-2" ref="text_85012" transform="translate(174.82 469.54)">85012</text>
              <text class="cls-2" ref="text_85015" transform="translate(153.27 475.01)">85015</text>
              <text class="cls-2" ref="text_85013" transform="translate(166.77 482.68)">85013</text>
              <text class="cls-2" ref="text_85017" transform="translate(135.77 461.11)">85017</text>
              <text class="cls-2" ref="text_85019" transform="translate(121.74 466.3)">85019</text>
              <text class="cls-2" ref="text_85037" transform="translate(31.96 482.68)">85037</text>
              <text class="cls-2" ref="text_85033" transform="translate(70.93 486.8)">85033</text>
              <text class="cls-2" ref="text_85031" transform="translate(103.77 482.68)">85031</text>
              <text class="cls-2" ref="text_85035" transform="translate(85.27 504.8)">85035</text>
              <text class="cls-2" ref="text_85043" transform="translate(88.24 533.11)">85043</text>
              <text class="cls-2" ref="text_85009" transform="translate(134.95 528.56)">85009</text>
              <text class="cls-2" ref="text_85007" transform="translate(159.27 525.37)">85007</text>
            </g>
          </svg>
        </div>
      </div>
    </div>
  </div>
</template>

<script>
import Panzoom from '@panzoom/panzoom'

const colorBetween = require('color-between')

export default {
  name: 'Map',
  props: {
    zips: {},
    low_color: {},
    high_color: {},
    title: {},
    population: {},
    ratio: {},
    text_color: {
      title: '',
      default: 'white'
    },
    scala: {
      default: ''
    }
  },
  data () {
    return {
      showZips: false,
      showPercent: false,
      el: null
    }
  },
  watch: {
    zips: {
      deep: true,
      immediate: true,
      handler () {
        console.log('hai')
        this.evaluteZips()
      }
    }
  },
  mounted () {
    this.el = Panzoom(document.querySelector('#map-' + this.title.replace(/ /g, '').replace(/&/g, '')))
    this.evaluteZips()
    // ugly panzoom fix
    const el = this.el
    this.$refs.parent.addEventListener('wheel', function (event) {
      el.zoomWithWheel(event)
    })
  },
  methods: {
    evaluteZips () {
      for (let zip in this.zips) {
        if (this.$refs.hasOwnProperty('_' + zip)) {
          // assign color
          this.$refs['_' + zip].style.fill = colorBetween(this.low_color, this.high_color, this.zips[zip])
          // assign Percent
        }
        if (this.$refs.hasOwnProperty('percent_' + zip)) {
          this.$refs['percent_' + zip].textContent = Number(this.zips[zip]).toFixed(2)
        }
      }
    }
  },
  computed: {

  }
}
</script>

<style scoped>
  .cls-1 {
    stroke: #000;
  }

  .svg-container {
    height: 75vh;
    width: 15vw;
  }

  .cls-2 {
    font-size: 7px;
    font-family: ArialMT, Arial;
  }
</style>
